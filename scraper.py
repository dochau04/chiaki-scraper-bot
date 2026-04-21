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

async def discover_links(page, category_url):
    all_links = set()
    # Châu cho chạy từ trang 1 đến 20 (hoặc hơn) để lấy thật nhiều
    for p in range(1, 21): 
        url = f"{category_url}?page={p}"
        try:
            print(f"🚀 [MASTER] Đang lật sang trang {p}: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3) # Đợi web load sản phẩm
            
            # Lấy link bằng đúng selector của Châu
            results = await page.evaluate('''() => {
                const selectors = ['.product-item a', '.name-product a', '.item-product a', 'h3 a'];
                let found = [];
                selectors.forEach(s => {
                    document.querySelectorAll(s).forEach(el => {
                        if (el.href && el.href.includes('chiaki.vn/') && !el.href.includes('tin-tuc')) {
                            found.push(el.href);
                        }
                    });
                });
                return found;
            }''')
            
            # QUAN TRỌNG: Nếu trang này không có sản phẩm nào thì mới dừng lật trang
            if not results or len(results) == 0:
                print(f"⏹️ Đã hết hàng ở danh mục này tại trang {p}")
                break
                
            for l in results:
                all_links.add(l)
                
            print(f"✅ Đã lấy thêm {len(results)} link ở trang {p}. Tổng: {len(all_links)}")
        except Exception as e:
            print(f"⚠️ Lỗi khi lật trang {p}: {e}")
            break
            
    return list(all_links)

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
                # Master lấy 10 danh mục "cũ nhất" hoặc "chưa quét"
                with psycopg2.connect(DB_URL) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC NULLS FIRST LIMIT 10")
                        categories = cur.fetchall()
                
                for cat in categories:
                    # Sau khi discover_links xong 20 trang của danh mục này...
                    links = await discover_links(page, cat['url'])
                    
                    if links:
                        # ...thì nó sẽ nạp vào DB
                        data = [(l, cat['category_name']) for l in links]
                        with psycopg2.connect(DB_URL) as conn:
                            with conn.cursor() as cur:
                                for i in range(0, len(data), 100):
                                    cur.executemany("INSERT INTO products (url, category_name, status) VALUES (%s, %s, 'pending') ON CONFLICT (url) DO NOTHING", data[i:i+100])
                                # Đánh dấu đã quét để lần sau Master lấy danh mục KHÁC
                                cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                                conn.commit()
                        print(f"🏁 ĐÃ XONG DANH MỤC: {cat['category_name']}. Chuyển sang mục tiếp theo...")
            finally:
                await browser.close()
        return

    await asyncio.sleep(20)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        while time.time() - START_TIME < MAX_RUNTIME:
            try:
                with psycopg2.connect(DB_URL) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("UPDATE products SET status = 'processing' WHERE id IN (SELECT id FROM products WHERE status = 'pending' ORDER BY id ASC LIMIT 20 FOR UPDATE SKIP LOCKED) RETURNING id, url;")
                        jobs = cur.fetchall()
                        if not jobs: 
                            await asyncio.sleep(30); continue
                        queue = asyncio.Queue()
                        for j in jobs: await queue.put(j)
                        results = []
                        tasks = [asyncio.create_task(worker_task(context, queue, results)) for _ in range(CONCURRENCY)]
                        await queue.join()
                        for t in tasks: t.cancel()
                        for info, j_id in results:
                            cur.execute("""UPDATE products SET product_name=%s, price_sale=%s, price_market=%s, image_link=%s, brand=%s, origin=%s, stock=%s, sold=%s, description=%s, kho_hang=%s, status='completed', updated_at=NOW() WHERE id=%s""",
                                (str(info['product_name']), str(info['price_sale']), str(info['price_market']), str(info['image_link']), str(info['brand']), str(info['origin']), str(info['stock']), str(info['sold']), str(info['description']), str(info['kho_hang']), j_id))
                        conn.commit()
            except Exception as e:
                await asyncio.sleep(10)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
