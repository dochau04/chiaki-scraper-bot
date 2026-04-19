import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor
import os

DB_URL = os.environ.get('DATABASE_URL')
CATEGORY_URL = "https://chiaki.vn/my-pham"

# Hàm lấy link từ trang danh mục (Hỗ trợ cuộn trang sâu)
async def discover_links(page, target=50000):
    print(f"🔎 Đang quét link từ: {CATEGORY_URL}")
    await page.goto(CATEGORY_URL, wait_until="domcontentloaded", timeout=60000)
    
    found_links = set()
    for _ in range(50): # Cuộn trang 50 lần để load thêm sản phẩm
        await page.mouse.wheel(0, 5000)
        await asyncio.sleep(2)
        
        new_links = await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('.product-item a, .item-product a'))
                        .map(a => a.href)
                        .filter(h => h.includes('chiaki.vn/') && !h.includes('tin-tuc'));
        }''')
        for l in new_links: found_links.add(l)
        print(f"📊 Đã tìm thấy: {len(found_links)} links")
        
        # Thử click nút xem thêm nếu có
        try:
            btn = await page.query_selector('text="Xem thêm"')
            if btn: await btn.click()
        except: pass
        
        if len(found_links) >= target: break
    return list(found_links)

async def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # 1. LẤY MỘT DANH MỤC ĐỂ QUÉT LINK SẢN PHẨM
        cur.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC NULLS FIRST LIMIT 1")
        cat = cur.fetchone()
        
        if cat:
            print(f"📂 Đang quét danh mục: {cat['category_name']}")
            product_links = await discover_links(page, cat['url']) # Dùng hàm quét link đã có
            
            for link in product_links:
                cur.execute("""
                    INSERT INTO products (url, category_name, status) 
                    VALUES (%s, %s, 'pending') 
                    ON CONFLICT (url) DO NOTHING
                """, (link, cat['category_name']))
            
            # Cập nhật thời gian vừa quét danh mục này xong
            cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
            conn.commit()

        # 2. SAU ĐÓ CÀO CHI TIẾT NHƯ BÌNH THƯỜNG
        cur.execute("SELECT id, url FROM products WHERE status = 'pending' LIMIT 20")
        jobs = cur.fetchall()
        
        for job in jobs:
            try:
                print(f"🚀 Cào SP: {job['url']}")
                # Chỗ này dùng hàm scrape_product_detail đã viết ở trên của bạn
                # (Lưu ý: Để tiết kiệm dung lượng, mình tóm lược logic cập nhật)
                # ... [Code trích xuất dữ liệu của bạn] ...
                
                # Cập nhật kết quả
                cur.execute("UPDATE products SET status = 'completed', updated_at = NOW() WHERE id = %s", (job['id'],))
                conn.commit()
            except: continue

        await browser.close()
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
