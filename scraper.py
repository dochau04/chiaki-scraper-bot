import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor
import os

DB_URL = os.environ.get('DATABASE_URL')

# --- HÀM 1: QUÉT TẤT CẢ LINKS TỪ TRANG DANH MỤC ---
async def discover_links(page, url):
    print(f"🔎 Đang quét link từ danh mục: {url}")
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    
    found_links = set()
    for i in range(20): 
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
            if btn: await btn.click(); await asyncio.sleep(2)
        except: pass
        
    print(f"📊 Tổng cộng tìm thấy: {len(found_links)} link sản phẩm.")
    return list(found_links)

# --- HÀM 2: BÓC TÁCH CHI TIẾT SẢN PHẨM ---
async def scrape_product_detail(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.mouse.wheel(0, 800)
        await asyncio.sleep(2)

        data = await page.evaluate('''() => {
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
            res.price_sale = document.querySelector('#price-show')?.innerText.replace(/[^0-9]/g, '') || 0;
            res.price_market = document.querySelector('#sale-price-show')?.innerText.replace(/[^0-9]/g, '') || res.price_sale;
            
            // THÔNG SỐ ROW (Thương hiệu, Xuất xứ, Kho, Tồn kho)
            document.querySelectorAll('.product-specs-row').forEach(row => {
                let lbl = row.querySelector('.product-specs-label')?.innerText.trim();
                let val = row.querySelector('.product-specs-value')?.innerText.trim();
                if (lbl === 'Thương hiệu') res.brand = val;
                if (lbl === 'Xuất xứ') res.origin = val;
                if (lbl === 'Số sản phẩm còn lại') res.stock = val.replace(/[^0-9]/g, '');
                if (lbl === 'Kho hàng tại') res.kho_hang = val;
            });

            // ĐÃ BÁN
            let soldEl = document.querySelector('.item-sold .item-count') || document.querySelector('.item-count');
            res.sold = soldEl ? soldEl.innerText.replace(/[^0-9]/g, '') : 0;

            // MÔ TẢ
            res.description = document.querySelector('#content-product')?.innerText.trim() || 'N/A';
            
            return res;
        }''')
        return data
    except Exception as e:
        print(f"❌ Lỗi link {url}: {e}")
        return None
    finally:
        await page.close()

# --- HÀM CHÍNH ---
async def main():
    if not DB_URL: return print("❌ Thiếu DATABASE_URL")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Mở kết nối tạm để lấy 1 danh mục
        temp_conn = psycopg2.connect(DB_URL)
        temp_cur = temp_conn.cursor(cursor_factory=RealDictCursor)
        temp_cur.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC NULLS FIRST LIMIT 1")
        cat = temp_cur.fetchone()
        temp_conn.close() 

        product_links = []
        if cat:
            product_links = await discover_links(page, cat['url']) 
        
        # MỞ KẾT NỐI LẠI ĐỂ LƯU DỮ LIỆU
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if cat and product_links:
            print(f"📥 Đang nạp {len(product_links)} link vào Database...")
            for link in product_links:
                cur.execute("""
                    INSERT INTO products (url, category_name, status) 
                    VALUES (%s, %s, 'pending') 
                    ON CONFLICT (url) DO NOTHING
                """, (link, cat['category_name']))
            
            cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
            conn.commit()

        # CÀO CHI TIẾT 20 SP
        cur.execute("SELECT id, url FROM products WHERE status = 'pending' LIMIT 20")
        jobs = cur.fetchall()
        
        for job in jobs:
            print(f"🚀 Đang cào chi tiết: {job['url']}")
            info = await scrape_product_detail(context, job['url'])
            
            if info:
                # ĐÃ ĐỒNG BỘ TÊN KEY VỚI HÀM SCRAPE Ở TRÊN
                cur.execute("""
                    UPDATE products SET 
                    product_name = %s, 
                    price_sale = %s, 
                    price_market = %s, 
                    image_link = %s, 
                    brand = %s, 
                    origin = %s, 
                    stock = %s, 
                    sold = %s, 
                    description = %s, 
                    kho_hang = %s,
                    status = 'completed', 
                    updated_at = NOW() 
                    WHERE id = %s
                """, (
                    info.get('product_name'),
                    info.get('price_sale'),
                    info.get('price_market'),
                    info.get('image_link'),   # Đã sửa từ 'Link ảnh' thành 'image_link'
                    info.get('brand'),        # Đã sửa từ 'Thương hiệu' thành 'brand'
                    info.get('origin'),       # Đã sửa từ 'Xuất xứ' thành 'origin'
                    info.get('stock', 0),     # Đã sửa từ 'Sản phẩm còn lại' thành 'stock'
                    info.get('sold', 0),      # Đã sửa từ 'Đã bán' thành 'sold'
                    info.get('description'),  # Đã sửa từ 'Mô tả' thành 'description'
                    info.get('kho_hang'),     # Đã sửa từ 'Kho hàng tại' thành 'kho_hang'
                    job['id']
                ))
                conn.commit()
                print(f"✅ Đã cập nhật chuẩn: {info.get('product_name')}")

        await browser.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
