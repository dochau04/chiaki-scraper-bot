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
CONCURRENCY = 5 

async def discover_links(page, url):
    print(f"🔎 [MASTER] Quét danh mục: {url}")
    try:
        await page.goto(url, wait_until="commit", timeout=60000)
        found_links = set()
        for i in range(20): 
            await page.mouse.wheel(0, 4000)
            await asyncio.sleep(0.8)
            new_links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('.product-item a, .item-product a'))
                            .map(a => a.href)
                            .filter(h => h.includes('chiaki.vn/') && !h.includes('tin-tuc'));
            }''')
            for l in new_links: found_links.add(l)
            if len(found_links) >= 1000: break
        return list(found_links)
    except Exception as e:
        print(f"❌ Lỗi quét link: {e}"); return []

async def scrape_product_detail(context, url):
    page = await context.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,css,woff,otf,svg}", lambda route: route.abort())
    try:
        await page.goto(url, wait_until="commit", timeout=30000)
        await asyncio.sleep(0.5)
        return await page.evaluate('''() => {
            const getT = (sel) => document.querySelector(sel)?.innerText.trim() || 'N/A';
            let res = {
                product_name: getT('h1') || getT('.title-product'),
                price_sale: document.querySelector('#price-show')?.innerText || '0',
                price_market: document.querySelector('#sale-price-show')?.innerText || '0',
                image_link: document.querySelector('.product-img-main img')?.src || 'N/A',
                brand: 'N/A', origin: 'N/A', stock: '0', sold: '0', kho_hang: 'N/A',
                description: getT('#content-product')
            };
            document.querySelectorAll('.product-specs-row').forEach(row => {
                let lbl = row.querySelector('.product-specs-label')?.innerText.trim();
                let val = row.querySelector('.product-specs-value')?.innerText.trim();
                if (lbl === 'Thương hiệu') res.brand = val;
                if (lbl === 'Xuất xứ') res.origin = val;
                if (lbl === 'Số sản phẩm còn lại') res.stock = val;
                if (lbl === 'Kho hàng tại') res.kho_hang = val;
            });
            let s = document.querySelector('.item-sold .item-count');
            if (s) res.sold = s.innerText;
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
            conn = psycopg2.connect(DB_URL)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC NULLS FIRST LIMIT 1")
            cat = cur.fetchone()
            if cat:
                links = await discover_links(page, cat['url'])
                if links:
                    data = [(l, cat['category_name']) for l in links]
                    for i in range(0, len(data), 100):
                        cur.executemany("INSERT INTO products (url, category_name, status) VALUES (%s, %s, 'pending') ON CONFLICT (url) DO NOTHING", data[i:i+100])
                    cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                    conn.commit()
            cur.close(); conn.close(); await browser.close()
        return

    await asyncio.sleep(60) 
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        while time.time() - START_TIME < MAX_RUNTIME:
            conn = psycopg2.connect(DB_URL)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("UPDATE products SET status = 'processing' WHERE id IN (SELECT id FROM products WHERE status = 'pending' ORDER BY id ASC LIMIT 40 FOR UPDATE SKIP LOCKED) RETURNING id, url;")
            jobs = cur.fetchall()
            conn.commit()
            if not jobs:
                cur.close(); conn.close(); await asyncio.sleep(30); continue
            
            queue = asyncio.Queue()
            for j in jobs: await queue.put(j)
            results = []
            tasks = [asyncio.create_task(worker_task(context, queue, results)) for _ in range(CONCURRENCY)]
            await queue.join()
            for t in tasks: t.cancel()

            for info, j_id in results:
                try:
                    cur.execute("""UPDATE products SET product_name=%s, price_sale=%s, price_market=%s, image_link=%s, brand=%s, origin=%s, stock=%s, sold=%s, description=%s, kho_hang=%s, status='completed', worker_id=%s, updated_at=NOW() WHERE id=%s""",
                        (str(info['product_name']), str(info['price_sale']), str(info['price_market']), str(info['image_link']), str(info['brand']), str(info['origin']), str(info['stock']), str(info['sold']), str(info['description']), str(info['kho_hang']), WORKER_ID, j_id))
                except: continue
            conn.commit(); cur.close(); conn.close()
            print(f"🚀 Worker {WORKER_ID} xong 40 link")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
