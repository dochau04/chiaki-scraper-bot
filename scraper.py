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
    p = 1
    consecutive_empty_pages = 0
    
    while p <= 200: # Cho lật thoải mái tới trang 200
        url = f"{category_url}?page={p}"
        try:
            print(f"🚀 [MASTER] Đang quét trang {p}: {url}")
            # Chuyển qua wait_until="domcontentloaded" cho nhanh, vì mình sleep sau đó rồi
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Cuộn chuột thật mạnh để kích hoạt render
            await page.mouse.wheel(0, 4000)
            await asyncio.sleep(4) # Tăng thời gian đợi một chút cho chắc
            
            # SELECTOR MỚI: Quét rộng hơn để không sót cái nào
            results = await page.evaluate('''() => {
                let found = [];
                // Quét tất cả thẻ a có cấu trúc link sản phẩm chiaki
                document.querySelectorAll('a').forEach(a => {
                    const href = a.href;
                    if (href.includes('-p-') && !href.includes('tin-tuc') && href.includes('chiaki.vn/')) {
                        found.push(href);
                    }
                });
                return found;
            }''')
            
            if not results or len(results) == 0:
                consecutive_empty_pages += 1
                print(f"⚠️ Trang {p} không thấy sản phẩm. Đang thử lại... ({consecutive_empty_pages}/3)")
                if consecutive_empty_pages >= 3:
                    break
                p += 1
                continue
            
            consecutive_empty_pages = 0
            before_size = len(all_links)
            for l in results: all_links.add(l)
            after_size = len(all_links)
            
            # In log chi tiết để Châu theo dõi
            if after_size > before_size:
                print(f"✅ Trang {p}: Bốc được {after_size - before_size} link mới. Tích lũy: {after_size}")
            else:
                print(f"⏩ Trang {p}: Không có link mới (toàn link cũ). Vẫn lật tiếp...")
            
            p += 1
        except Exception as e:
            print(f"⚠️ Lỗi trang {p}: {e}")
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
    
    # --- PHẦN MASTER (WORKER 1) ---
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
                            links = await discover_links(page, cat['url'])
                            if links:
                                data = [(l, cat['category_name']) for l in links]
                                for i in range(0, len(data), 100):
                                    cur.executemany("INSERT INTO products (url, category_name, status) VALUES (%s, %s, 'pending') ON CONFLICT (url) DO NOTHING", data[i:i+100])
                                cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                                conn.commit()
                                print(f"✅ Master nạp xong mục: {cat['category_name']}")
            except Exception as e:
                print(f"❌ Master lỗi: {e}")
                if 'conn' in locals(): conn.rollback()
            finally: await browser.close()
        return

    # --- PHẦN WORKER (2-10) ---
    await asyncio.sleep(20)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        
        # Mở kết nối 1 lần duy nhất cho cả quá trình Worker chạy
        try:
            with psycopg2.connect(DB_URL) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    while time.time() - START_TIME < MAX_RUNTIME:
                        # Lấy job
                        cur.execute("UPDATE products SET status = 'processing' WHERE id IN (SELECT id FROM products WHERE status = 'pending' ORDER BY id ASC LIMIT 20 FOR UPDATE SKIP LOCKED) RETURNING id, url;")
                        jobs = cur.fetchall()
                        conn.commit()

                        # NẾU KHÔNG CÒN LINK PENDING THÌ NGHỈ LUÔN
                        if not jobs:
                            print(f"🏁 Worker {WORKER_ID}: Đã hết link pending. Đang đóng máy nghỉ ngơi...")
                            break

                        queue = asyncio.Queue()
                        for j in jobs: await queue.put(j)
                        results = []
                        tasks = [asyncio.create_task(worker_task(context, queue, results)) for _ in range(CONCURRENCY)]
                        await queue.join()
                        for t in tasks: t.cancel()

                        # Lưu dữ liệu (Đã thêm worker_id ở đây)
                        for info, j_id in results:
                            cur.execute("""UPDATE products SET product_name=%s, price_sale=%s, price_market=%s, image_link=%s, brand=%s, origin=%s, stock=%s, sold=%s, description=%s, kho_hang=%s, status='completed', worker_id=%s, updated_at=NOW() WHERE id=%s""",
                                (str(info['product_name']), str(info['price_sale']), str(info['price_market']), str(info['image_link']), str(info['brand']), str(info['origin']), str(info['stock']), str(info['sold']), str(info['description']), str(info['kho_hang']), WORKER_ID, j_id))
                        conn.commit()
                        print(f"✅ Worker {WORKER_ID} cào xong 20 sản phẩm.")
        except Exception as e:
            print(f"⚠️ Worker {WORKER_ID} gặp lỗi: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
