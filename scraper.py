import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import os
import time

DB_URL = os.environ.get('DATABASE_URL')
WORKER_ID = os.environ.get('worker_id', '1')
START_TIME = time.time()
MAX_RUNTIME = 5.5 * 3600  # Chạy tối đa 5.5 tiếng để an toàn

# --- HÀM QUÉT LINK ---
async def discover_links(page, url):
    print(f"🔎 [MASTER] Đang quét link từ: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        found_links = set()
        for i in range(25): 
            await page.mouse.wheel(0, 4000)
            await asyncio.sleep(2)
            new_links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('.product-item a, .item-product a'))
                            .map(a => a.href)
                            .filter(h => h.includes('chiaki.vn/') && !h.includes('tin-tuc'));
            }''')
            for l in new_links: found_links.add(l)
            try:
                btn = await page.query_selector('text="Xem thêm"')
                if btn: await btn.click()
            except: pass
            if len(found_links) >= 1000: break
        return list(found_links)
    except Exception as e:
        print(f"❌ Lỗi quét link: {e}")
        return []

# --- HÀM CÀO CHI TIẾT (ĐÃ ĐỒNG BỘ KEY) ---
async def scrape_product_detail(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(2)

        data = await page.evaluate('''() => {
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
        return data
    except: return None
    finally: await page.close()

async def main():
    if not DB_URL: return
    
    # --- BƯỚC 1: MASTER NẠP MỒI ---
    if WORKER_ID == '1':
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC LIMIT 1")
        cat = cur.fetchone()
        
        if cat:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                links = await discover_links(page, cat['url'])
                await browser.close()
                
                if links:
                    print(f"📥 [MASTER] Nạp {len(links)} link mồi (Chia nhỏ để tránh Timeout)...")
                    data = [(l, cat['category_name'], 'pending') for l in links]
                    
                    # Chia nhỏ data thành các nhóm 100 cái một
                    # Chia nhỏ data thành các nhóm 50 cái một để tránh lỗi mạng
                    batch_size = 50 
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        execute_values(cur, """
                            INSERT INTO products (url, category_name, status) 
                            VALUES %s ON CONFLICT (url) DO NOTHING
                        """, batch)
                        conn.commit() # Lưu ngay từng đợt để giải phóng bộ nhớ
                        
                        # QUAN TRỌNG: Nghỉ 1 giây để tránh nghẽn đường truyền SSL/Timeout
                        await asyncio.sleep(1) 
                        print(f"✅ Master đã nạp xong nhóm {i//batch_size + 1}")

                    cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                    conn.commit()
        cur.close(); conn.close()
        return

    # --- BƯỚC 2: WORKER CÀO LIÊN TỤC (VÒNG LẶP) ---
    print(f"⏳ [WORKER {WORKER_ID}] Đợi Master 5 phút...")
    await asyncio.sleep(300)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        
        while time.time() - START_TIME < MAX_RUNTIME:
            conn = psycopg2.connect(DB_URL)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                UPDATE products SET status = 'processing', updated_at = NOW()
                WHERE id IN (
                    SELECT id FROM products WHERE status = 'pending' 
                    ORDER BY id ASC LIMIT 20 FOR UPDATE SKIP LOCKED
                ) RETURNING id, url;
            """)
            jobs = cur.fetchall()
            conn.commit()

            if not jobs:
                print(f"📭 [WORKER {WORKER_ID}] Hết việc.")
                cur.close(); conn.close()
                break

            for job in jobs:
                info = await scrape_product_detail(context, job['url'])
                if info:
                    cur.execute("""
                        UPDATE products SET 
                        product_name = %s, price_sale = %s, price_market = %s, 
                        image_link = %s, brand = %s, origin = %s, stock = %s, 
                        sold = %s, description = %s, kho_hang = %s,
                        status = 'completed', updated_at = NOW() 
                        WHERE id = %s
                    """, (
                        info.get('product_name'), info.get('price_sale'), info.get('price_market'),
                        info.get('image_link'), info.get('brand'), info.get('origin'),
                        info.get('stock', 0), info.get('sold', 0), info.get('description'),
                        info.get('kho_hang'), job['id']
                    ))
                    conn.commit()
            
            cur.close(); conn.close()
            print(f"✅ [WORKER {WORKER_ID}] Xong 1 đợt 20 sản phẩm.")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
