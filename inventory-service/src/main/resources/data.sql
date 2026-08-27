INSERT INTO products (product_id, name, stock) VALUES
    ('P100', 'Wireless Mouse', 50),
    ('P101', 'Mechanical Keyboard', 20),
    ('P102', 'USB-C Hub', 2)
ON CONFLICT (product_id) DO NOTHING;
