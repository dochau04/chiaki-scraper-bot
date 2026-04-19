import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import os

DB_URL = os.environ.get('DATABASE_URL')
WORKER_ID = os.environ.get('worker_id', '1')

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

async def scrape_product_detail(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(2)
        return await page.evaluate('''() => {
            const getT = (sel) => document.querySelector(sel)?.innerText.trim() || 'N/A';
            return {
                product_name: getT('h1'),
                price_sale: document.querySelector('#price-show')?.innerText.replace(/[^0-9]/g, '') || 0,
                price_market: document.querySelector('#sale-price-show')?.innerText.replace(/[^0-9]/g, '') || 0,
                image_link: document.querySelector('.product-img-main')?.getAttribute('src') || 'N/A',
                description: document.querySelector('#content-product')?.innerText.trim() || 'N/A'
            };
        }''')
    except: return None
    finally: await page.close()

async def main():
    if not DB_URL: return
    
    # --- BƯỚC 1: PHÂN CHIA VAI TRÒ ---
    if WORKER_ID == '1':
        # VAI TRÒ MASTER: CHỈ QUÉT LINK
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
                    print(f"📥 [MASTER] Nạp {len(links)} link. Giải phóng DB ngay...")
                    data_to_insert = [(l, cat['category_name'], 'pending') for l in links]
                    execute_values(cur, "INSERT INTO products (url, category_name, status) VALUES %s ON CONFLICT (url) DO NOTHING", data_to_insert)
                    cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                    conn.commit()
        cur.close()
        conn.close()
        print("🏁 [MASTER] Đã hoàn thành nhiệm vụ rải link. Kết thúc.")
        return # Worker 1 làm xong thì nghỉ luôn, không cào để nhường chỗ cho con khác

    else:
        # VAI TRÒ WORKER (CON 2 ĐẾN 10): CHỈ CÀO CHI TIẾT
        # Tăng thời gian đợi lên 300s (5 phút) để Master kịp quét 25 lần cuộn chuột
        print(f"⏳ [WORKER {WORKER_ID}] Đợi Master nạp hàng (300s)...")
        await asyncio.sleep(300)

        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Bốc 50 hoặc 100 dòng tùy bạn
        cur.execute("""
            UPDATE products SET status = 'processing', updated_at = NOW()
            WHERE id IN (
                SELECT id FROM products WHERE status = 'pending' 
                ORDER BY id ASC LIMIT 100 FOR UPDATE SKIP LOCKED
            ) RETURNING id, url;
        """)
        jobs = cur.fetchall()
        conn.commit()

        if jobs:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                print(f"🚀 [WORKER {WORKER_ID}] Nhận {len(jobs)} việc.")
                for job in jobs:
                    info = await scrape_product_detail(context, job['url'])
                    if info:
                        cur.execute("""
                            UPDATE products SET product_name = %s, price_sale = %s, price_market = %s, 
                            image_link = %s, description = %s, status = 'completed', updated_at = NOW() 
                            WHERE id = %s
                        """, (info['product_name'], info['price_sale'], info['price_market'],
                              info['image_link'], info['description'], job['id']))
                        conn.commit()
                await browser.close()
        else:
            print(f"📭 [WORKER {WORKER_ID}] Không tìm thấy việc (Master nạp chưa xong hoặc kho đã hết).")
        
        cur.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
