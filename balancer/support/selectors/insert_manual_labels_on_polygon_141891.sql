-- part of a query repo
-- query name: Insert manual labels on Polygon
-- query link: https://dune.com/queries/141891


DROP TABLE IF EXISTS dune_user_generated.balancer_manual_labels;

CREATE TABLE dune_user_generated.balancer_manual_labels (
    address bytea,
    author text,
    name text,
    type text
);

INSERT INTO dune_user_generated.balancer_manual_labels VALUES
('\xBA12222222228d8Ba445958a75a0704d566BF2C8', 'balancerlabs', 'vault', 'balancer_source'),
('\x11111112542d85b3ef69ae05771c2dccff4faa26', 'balancerlabs', '1inch', 'balancer_source'),
('\x1111111254fb6c44bac0bed2854e76f90643097d', 'balancerlabs', '1inch', 'balancer_source'),
('\xdef1c0ded9bec7f1a1670819833240f027b25eff', 'balancerlabs', 'matcha', 'balancer_source'),
('\xF2e4209afA4C3c9eaA3Fb8e12eeD25D8f328171C', 'balancerlabs', 'slingshot', 'balancer_source'),
('\xdef171fe48cf0115b1d80b88dc8eab59176fee57', 'balancerlabs', 'paraswap', 'balancer_source'),
('\x1a1ec25dc08e98e5e93f1104b5e5cdd298707d31', 'balancerlabs', 'metamask', 'balancer_source');

SELECT * FROM dune_user_generated.balancer_manual_labels