import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import time

DB_URL = os.environ.get('DATABASE_URL')
WORKER_ID = os.environ.get('worker_id', '1')
START_TIME = time.time()
MAX_RUNTIME = 5.5 * 3600
CONCURRENCY = 5  # Số tab chạy song song trong mỗi Worker (Tăng lên để nhanh hơn)

async def discover_links(page, url):
    print(f"🔎 [MASTER] Đang quét link từ: {url}")
    try:
        # Tắt chặn tài nguyên để Master quét link nhanh hơn
        await page.goto(url, wait_until="commit", timeout=60000)
        found_links = set()
        for i in range(25): 
            await page.mouse.wheel(0, 5000)
            await asyncio.sleep(1) # Giảm thời gian chờ
            new_links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('.product-item a, .item-product a'))
                            .map(a => a.href)
                            .filter(h => h.includes('chiaki.vn/') && !h.includes('tin-tuc'));
            }''')
            for l in new_links: found_links.add(l)
            if len(found_links) >= 1200: break
        return list(found_links)
    except Exception as e:
        print(f"❌ Lỗi quét link: {e}"); return []

async def scrape_product_detail(context, url):
    page = await context.new_page()
    # CHIÊU 1: Tắt load ảnh/css/font để tiết kiệm 80% thời gian và băng thông
    await page.route("**/*.{png,jpg,jpeg,gif,css,woff,otf,svg}", lambda route: route.abort())
    try:
        # CHIÊU 2: Chỉ chờ đến khi 'commit' (nhận được html) là xử lý luôn
        await page.goto(url, wait_until="commit", timeout=30000)
        await asyncio.sleep(0.5) # Chờ một chút cho JS chạy
        return await page.evaluate('''() => {
            const getT = (sel) => document.querySelector(sel)?.innerText.trim() || 'N/A';
            let res = {};

            // Đặt tên Key tiếng Anh để khớp với SQL
            let imgEl = document.querySelector('.product-img-main') || 
                        document.querySelector('.lg-item img') ||
                        document.querySelector('#product-image-feature');
            res.image_link = imgEl ? imgEl.getAttribute('src') : 'N/A';

            res.product_name = getT('h1') || getT('.title-product');

            res.price_sale = document.querySelector('#price-show')?.innerText.replace(/[^0-9]/g, '') || 0;
            res.price_market = document.querySelector('#sale-price-show')?.innerText.replace(/[^0-9]/g, '') || res.price_sale;

            document.querySelectorAll('.product-specs-row').forEach(row => {
                let lbl = row.querySelector('.product-specs-label')?.innerText.trim();
                let val = row.querySelector('.product-specs-value')?.innerText.trim();
                if (lbl === 'Thương hiệu') res.brand = val;
                if (lbl === 'Xuất xứ') res.origin = val;
                if (lbl === 'Số sản phẩm còn lại') res.stock = val.replace(/[^0-9]/g, '');
                if (lbl === 'Kho hàng tại') res.kho_hang = val;
            });

            let soldEl = document.querySelector('.item-sold .item-count') || document.querySelector('.item-count');
            res.sold = soldEl ? soldEl.innerText.replace(/[^0-9]/g, '') : 0;
            res.description = document.querySelector('#content-product')?.innerText.trim() || 'N/A';

            return res;
        }''')
    except: return None
    finally: await page.close()

async def worker_task(context, job_queue, results):
    """Hàm xử lý dành cho từng tab song song"""
    while not job_queue.empty():
        job = await job_queue.get()
        info = await scrape_product_detail(context, job['url'])
        if info:
            results.append((info, job['id']))
        job_queue.task_done()

async def main():
    if not DB_URL: return
    
    if WORKER_ID == '1':
        # --- MASTER GIỮ NGUYÊN LOGIC NHANH ---
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            conn_init = psycopg2.connect(DB_URL)
            cur_init = conn_init.cursor(cursor_factory=RealDictCursor)
            cur_init.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC LIMIT 1")
            cat = cur_init.fetchone()
            cur_init.close(); conn_init.close()
            
            if cat:
                links = await discover_links(page, cat['url'])
                await browser.close()
                if links:
                    conn = psycopg2.connect(DB_URL)
                    cur = conn.cursor()
                    data = [(l, cat['category_name']) for l in links]
                    for i in range(0, len(data), 100):
                        cur.executemany("INSERT INTO products (url, category_name, status) VALUES (%s, %s, 'pending') ON CONFLICT (url) DO NOTHING", data[i:i+100])
                        conn.commit()
                    cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                    conn.commit(); cur.close(); conn.close()
        return

    # --- WORKER: CHẠY SONG SONG NHIỀU TAB ---
    await asyncio.sleep(200) # Đợi Master rải link xong
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        
        while time.time() - START_TIME < MAX_RUNTIME:
            conn = psycopg2.connect(DB_URL)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            # Bốc 50 sản phẩm mỗi lần để cào song song
            cur.execute("""
                UPDATE products SET status = 'processing', updated_at = NOW()
                WHERE id IN (
                    SELECT id FROM products WHERE status = 'pending' 
                    ORDER BY id ASC LIMIT 50 FOR UPDATE SKIP LOCKED
                ) RETURNING id, url;
            """)
            jobs = cur.fetchall()
            conn.commit()

            if not jobs:
                cur.close(); conn.close(); await asyncio.sleep(30); continue

            # CHIÊU 3: Chia 50 jobs cho 5 tab chạy song song
            queue = asyncio.Queue()
            for j in jobs: await queue.put(j)
            
            results = []
            tasks = [asyncio.create_task(worker_task(context, queue, results)) for _ in range(CONCURRENCY)]
            await queue.join()
            for t in tasks: t.cancel()

            # Lưu kết quả hàng loạt
            for info, j_id in results:
                cur.execute("""
                    UPDATE products SET product_name = %s, price_sale = %s, price_market = %s, 
                    brand = %s, origin = %s, stock = %s, status = 'completed', updated_at = NOW() 
                    WHERE id = %s
                """, (info['product_name'], info['price_sale'], info['price_market'],
                      info['brand'], info['origin'], info['stock'], j_id))
            conn.commit()
            cur.close(); conn.close()
            print(f"⚡ [WORKER {WORKER_ID}] Vừa băm xong 50 link!")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
