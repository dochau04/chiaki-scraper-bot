import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Kết nối lấy từ Secret DATABASE_URL mà bạn đã cài trên GitHub
DB_URL = os.environ.get('DATABASE_URL')

async def scrape_one_product(context, url):
    page = await context.new_page()
    try:
        # Tăng timeout lên 60s và đợi trang tải xong phần cốt lõi
        print(f"🔗 Đang truy cập: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Cuộn trang nhẹ để kích hoạt tải ảnh và giá (nếu có lazy load)
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(3)

        # Trích xuất dữ liệu trực tiếp từ trình duyệt
        data = await page.evaluate('''() => {
            const getT = (sel) => document.querySelector(sel)?.innerText.trim() || 'N/A';
            let res = {};
            
            // Lấy ảnh sản phẩm
            let imgEl = document.querySelector('.product-img-main') || document.querySelector('.lg-item img');
            res.image_link = imgEl ? imgEl.getAttribute('src') : 'N/A';
            
            // Lấy tên sản phẩm
            res.product_name = getT('h1') || getT('.title-product');
            
            // Lấy giá bán và giá thị trường (xử lý bỏ chữ đ và dấu chấm)
            res.price_sale = document.querySelector('#price-show')?.innerText.replace(/[^0-9]/g, '') || 0;
            res.price_market = document.querySelector('#sale-price-show')?.innerText.replace(/[^0-9]/g, '') || res.price_sale;
            
            // Lấy các thông số kỹ thuật trong bảng
            document.querySelectorAll('.product-specs-row').forEach(row => {
                let lbl = row.querySelector('.product-specs-label')?.innerText.trim();
                let val = row.querySelector('.product-specs-value')?.innerText.trim();
                if (lbl === 'Thương hiệu') res.brand = val;
                if (lbl === 'Xuất xứ') res.origin = val;
                if (lbl === 'Số sản phẩm còn lại') res.stock = val.replace(/[^0-9]/g, '');
                if (lbl === 'Kho hàng tại') res.kho_hang = val;
            });

            // Lấy số lượng đã bán
            let soldEl = document.querySelector('.item-sold .item-count');
            res.sold = soldEl ? soldEl.innerText.replace(/[^0-9]/g, '') : 0;
            
            // Lấy mô tả sản phẩm
            res.description = document.querySelector('#content-product')?.innerText.trim() || 'N/A';
            
            return res;
        }''')
        return data
    except Exception as e:
        print(f"❌ Lỗi khi cào link: {e}")
        return None
    finally:
        await page.close()

async def main():
    if not DB_URL:
        print("❌ LỖI: DATABASE_URL chưa được thiết lập trong GitHub Secrets!")
        return
        
    # Kết nối tới Supabase
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Lấy ra 1 link có trạng thái 'pending' để xử lý
    cur.execute("SELECT id, url FROM products WHERE status = 'pending' LIMIT 1")
    job = cur.fetchone()
    
    if not job:
        print("📭 Hết link 'pending' rồi Châu ơi. Hãy nạp thêm link vào Database!")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        
        print(f"🚀 Bắt đầu xử lý: {job['url']}")
        
        # Cập nhật trạng thái sang 'processing' để tránh trùng lặp
        cur.execute("UPDATE products SET status = 'processing' WHERE id = %s", (job['id'],))
        conn.commit()
        
        result = await scrape_one_product(context, job['url'])
        
        if result and result['product_name'] != 'N/A':
            # Cập nhật thông tin chi tiết về Supabase
            cur.execute("""
                UPDATE products SET 
                product_name = %s, price_sale = %s, price_market = %s, 
                image_link = %s, brand = %s, origin = %s, stock = %s, 
                sold = %s, description = %s, kho_hang = %s,
                status = 'completed', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (result['product_name'], result['price_sale'], result['price_market'],
                  result['image_link'], result.get('brand', 'N/A'), result.get('origin', 'N/A'),
                  result.get('stock', 0), result.get('sold', 0), result['description'],
                  result.get('kho_hang', 'N/A'), job['id']))
            conn.commit()
            print(f"✅ Đã lưu thành công sản phẩm: {result['product_name']}")
        else:
            # Nếu lỗi, trả về trạng thái 'pending' để thử lại lần sau
            cur.execute("UPDATE products SET status = 'pending' WHERE id = %s", (job['id'],))
            conn.commit()
            print(f"⚠️ Không lấy được dữ liệu, đã reset trạng thái link.")
        
        await browser.close()
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
