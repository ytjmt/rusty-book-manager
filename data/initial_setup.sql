INSERT INTO
  roles(name)
VALUES
  ('Admin'),
  ('User') ON CONFLICT DO NOTHING;

INSERT INTO
  users (name, email, password_hash, role_id)
SELECT
  'Eleazar Fig',
  'eleazar.fig@example.com',
  '$2b$12$Gzj2ihXuekcVsVmUzJ1B2OjuR61YmDMdH9YYuOQg9d4W26DEHYTWm',
  role_id
FROM
  roles
WHERE
  name LIKE 'Admin';
