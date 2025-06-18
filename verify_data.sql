-- Queries to verify data loading in PostgreSQL for the ArtisanMarket project

-- 1. Check the row counts for each table
SELECT 'categories' as table_name, count(*) FROM categories
UNION ALL
SELECT 'sellers', count(*) FROM sellers
UNION ALL
SELECT 'users', count(*) FROM users
UNION ALL
SELECT 'products', count(*) FROM products;

-- 2. Inspect some sample data from each table
SELECT * FROM categories LIMIT 5;

SELECT * FROM sellers LIMIT 5;

SELECT * FROM users LIMIT 5;

SELECT * FROM products LIMIT 5;


-- 3. Verify foreign key relationships with a JOIN
SELECT
    p.name AS product_name,
    p.price,
    c.name AS category_name,
    s.name AS seller_name
FROM
    products p
JOIN
    categories c ON p.category_id = c.id
JOIN
    sellers s ON p.seller_id = s.id
LIMIT 10; 