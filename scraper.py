import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import time

DB_URL = os.environ.get('DATABASE_URL')
WORKER_ID = os.environ.get('worker_id', '1')
START_TIME = time.time()
MAX_RUNTIME = 0.8 * 3600
CONCURRENCY = 3 

# --- HÀM LẤY LINK TỪ 1 TRANG DUY NHẤT (ĐỂ PHÂN PHỐI NHANH) ---
async def discover_links_single_page(page, category_url, p):
    try:
        url = f"{category_url}?page={p}"
        print(f"🚀 [MASTER] Đang bốc hàng trang {p}: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Đợi khung sản phẩm hiện ra
        try:
            await page.wait_for_selector('.list-product, #load_data_product', timeout=10000)
        except: pass

        await page.mouse.wheel(0, 3500)
        await asyncio.sleep(4) 
        
        results = await page.evaluate('''() => {
            let found = [];
            const container = document.querySelector('.list-product') || document.querySelector('#load_data_product');
            if (container) {
                container.querySelectorAll('a').forEach(a => {
                    if (a.href.includes('-p-') && !a.href.includes('tin-tuc')) {
                        found.push(a.href);
                    }
                });
            }
            return found;
        }''')
        return results
    except Exception as e:
        print(f"⚠️ Lỗi Master tại trang {p}: {e}")
        return []
async def scrape_product_detail(context, url):
    page = await context.new_page()
    try:
        # CHUẨN GỐC: Đợi domcontentloaded và scroll để trigger nạp data
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(3) # Đợi web render bảng thông số

        return await page.evaluate('''() => {
            const getT = (sel) => document.querySelector(sel)?.innerText.trim() || 'N/A';
            let res = {};

            // ẢNH SẢN PHẨM
            let imgEl = document.querySelector('.product-img-main') || 
                        document.querySelector('.lg-item img') ||
                        document.querySelector('#product-image-feature');
            res.image_link = imgEl ? imgEl.getAttribute('src') : 'N/A';

            // TÊN
            res.product_name = getT('h1') || getT('.title-product');

            // GIÁ
            let sPrice = document.querySelector('#price-show')?.innerText.replace(/[^0-9]/g, '') || '0';
            let mPrice = document.querySelector('#sale-price-show')?.innerText.replace(/[^0-9]/g, '') || sPrice;
            res.price_sale = sPrice;
            res.price_market = mPrice;

            // THÔNG SỐ ROW (Logic chuẩn của Châu)
            let stk = '0', brd = 'N/A', ori = 'N/A', wh = 'N/A';
            document.querySelectorAll('.product-specs-row').forEach(row => {
                let lbl = row.querySelector('.product-specs-label')?.innerText.trim();
                let val = row.querySelector('.product-specs-value')?.innerText.trim();
                if (lbl === 'Số sản phẩm còn lại') stk = val.replace(/[^0-9]/g, '');
                if (lbl === 'Thương hiệu') brd = val;
                if (lbl === 'Xuất xứ') ori = val;
                if (lbl === 'Kho hàng tại') wh = val;
            });
            res.stock = stk;
            res.brand = brd;
            res.origin = ori;
            res.kho_hang = wh;

            // ĐÃ BÁN & MÔ TẢ
            let soldEl = document.querySelector('.item-sold .item-count') || document.querySelector('.item-count');
            res.sold = soldEl ? soldEl.innerText.replace(/[^0-9]/g, '') : '0';
            res.description = document.querySelector('#content-product')?.innerText.trim() || 'N/A';

            return res;
        }''')
    except: return None
    finally: await page.close()

async def worker_task(context, job_queue, results):
    while not job_queue.empty():
        job = await job_queue.get()
        info = await scrape_product_detail(context, job['url'])
        if info: results.append((info, job['id']))
        job_queue.task_done()

async def main():
    if not DB_URL: return
    
    if WORKER_ID == '1':
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                with psycopg2.connect(DB_URL) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC NULLS FIRST LIMIT 5")
                        categories = cur.fetchall()
                        for cat in categories:
                            pg = 23 # Bắt đầu từ trang Châu muốn
                            empty_streak = 0
                            while pg <= 250:
                                links = await discover_links_single_page(page, cat['url'], pg)
                                if links:
                                    empty_streak = 0
                                    data = [(l, cat['category_name']) for l in links]
                                    cur.executemany("INSERT INTO products (url, category_name, status) VALUES (%s, %s, 'pending') ON CONFLICT (url) DO NOTHING", data)
                                    conn.commit() # PHÂN PHỐI HÀNG NGAY LẬP TỨC
                                    print(f"📦 [MASTER] Đã ném {len(links)} link trang {pg} vào kho.")
                                    pg += 1
                                else:
                                    empty_streak += 1
                                    if empty_streak >= 3: break
                                    pg += 1
                            cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                            conn.commit()
            finally: await browser.close()
        return

    # --- WORKER (2-10) ---
    await asyncio.sleep(int(WORKER_ID) * 5) 
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        try:
            with psycopg2.connect(DB_URL) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    empty_count = 0
                    while time.time() - START_TIME < MAX_RUNTIME:
                        cur.execute("UPDATE products SET status = 'processing' WHERE id IN (SELECT id FROM products WHERE status = 'pending' ORDER BY id ASC LIMIT 20 FOR UPDATE SKIP LOCKED) RETURNING id, url;")
                        jobs = cur.fetchall()
                        conn.commit()
                        if not jobs:
                            empty_count += 1
                            if empty_count > 20: break
                            await asyncio.sleep(30); continue
                        empty_count = 0
                        queue = asyncio.Queue(); [await queue.put(j) for j in jobs]
                        results = []
                        tasks = [asyncio.create_task(worker_task(context, queue, results)) for _ in range(CONCURRENCY)]
                        await queue.join()
                        for info, j_id in results:
                            cur.execute("""UPDATE products SET product_name=%s, price_sale=%s, price_market=%s, image_link=%s, brand=%s, origin=%s, stock=%s, sold=%s, description=%s, kho_hang=%s, status='completed', worker_id=%s, updated_at=NOW() WHERE id=%s""",
                                (str(info['product_name']), str(info['price_sale']), str(info['price_market']), str(info['image_link']), str(info['brand']), str(info['origin']), str(info['stock']), str(info['sold']), str(info['description']), str(info['kho_hang']), WORKER_ID, j_id))
                        conn.commit()
                        print(f"✅ Worker {WORKER_ID} cào xong 20 sản phẩm.")
        finally: await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
