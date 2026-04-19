import asyncio
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import RealDictCursor
import os

DB_URL = os.environ.get('DATABASE_URL')

# --- HÀM 1: QUÉT TẤT CẢ LINKS TỪ TRANG DANH MỤC ---
async def discover_links(page, url):
    print(f"🔎 [DISCOVERY] Đang quét link từ danh mục: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        found_links = set()
        # Cuộn trang để load lazy content
        for i in range(15): 
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
            
        print(f"📊 [DISCOVERY] Tìm thấy tổng cộng: {len(found_links)} link sản phẩm.")
        return list(found_links)
    except Exception as e:
        print(f"❌ [DISCOVERY] Lỗi khi quét trang tổng {url}: {e}")
        return []

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
    except Exception as e:
        print(f"❌ [SCRAPER] Lỗi cào chi tiết {url}: {e}")
        return None
    finally:
        await page.close()

# --- HÀM CHÍNH ---
# --- TRONG HÀM main() ---

async def main():
    if not DB_URL: return print("❌ Lỗi: Thiếu DATABASE_URL")
    
    # Lấy worker_id từ môi trường (GitHub Matrix truyền vào)
    # Nếu chạy thủ công thì mặc định là 1
    worker_id = os.environ.get('worker_id', '1')

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 🚀 CHỈ CHO PHÉP WORKER SỐ 1 ĐI QUÉT LINK DANH MỤC
        if worker_id == '1':
            cur.execute("SELECT id, url, category_name FROM categories ORDER BY last_scanned ASC NULLS FIRST LIMIT 1")
            cat = cur.fetchone()
            
            if cat:
                links = await discover_links(page, cat['url'])
                if links:
                    print(f"📥 [DB] Worker 1 đang nạp {len(links)} link mới...")
                    for l in links:
                        cur.execute("""
                            INSERT INTO products (url, category_name, status) 
                            VALUES (%s, %s, 'pending') 
                            ON CONFLICT (url) DO NOTHING
                        """, (l, cat['category_name']))
                    cur.execute("UPDATE categories SET last_scanned = NOW() WHERE id = %s", (cat['id'],))
                    conn.commit()
        else:
            print(f"🤖 Worker {worker_id} bỏ qua bước quét danh mục, đi cào chi tiết ngay...")

        # --- BƯỚC 2: BỐC NHIỆM VỤ PHÂN TÁN (SKIP LOCKED) ---
        # Mỗi Worker sẽ bốc 50 sản phẩm đang 'pending' hoặc bị kẹt hơn 30p
        print("🤖 [WORKER] Đang bốc nhiệm vụ từ hàng đợi...")
        cur.execute("""
            UPDATE products 
            SET status = 'processing', updated_at = NOW()
            WHERE id IN (
                SELECT id FROM products 
                WHERE status = 'pending' 
                OR (status = 'processing' AND updated_at < NOW() - INTERVAL '30 minutes')
                ORDER BY updated_at ASC NULLS FIRST 
                LIMIT 50 
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, url;
        """)
        jobs = cur.fetchall()
        conn.commit() # Commit ngay để khóa các dòng này lại, không cho Bot khác bốc trùng

        if not jobs:
            print("📭 [WORKER] Hàng đợi trống. Không còn việc để làm.")
        else:
            print(f"🚀 [WORKER] Bắt đầu cào {len(jobs)} sản phẩm song song...")
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
                    print(f"✅ [DONE] {info.get('product_name')}")

        await browser.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
