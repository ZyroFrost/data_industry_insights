-- 1. Tạo user
CREATE ROLE readonly_user
LOGIN
PASSWORD '77779999';

-- 2. Cho phép connect database
GRANT CONNECT ON DATABASE postgres TO readonly_user;

-- 3. Cho phép dùng schema public
GRANT USAGE ON SCHEMA public TO readonly_user;

-- 4. Cho SELECT tất cả table hiện tại
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

-- 5. Cho SELECT các table tạo sau này
ALTER DEFAULT PRIVILEGES
IN SCHEMA public
GRANT SELECT ON TABLES TO readonly_user;