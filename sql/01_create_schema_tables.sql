USE PortfolioSQL;
GO

CREATE TABLE olist.orders (
  order_id                varchar(50) NOT NULL,
  customer_id             varchar(50) NOT NULL,
  order_status            varchar(20) NULL,
  order_purchase_ts       datetime2(0) NULL,
  order_approved_ts       datetime2(0) NULL,
  order_delivered_carrier_ts datetime2(0) NULL,
  order_delivered_customer_ts datetime2(0) NULL,
  order_estimated_delivery_ts datetime2(0) NULL,
  CONSTRAINT PK_orders PRIMARY KEY (order_id)
);

CREATE TABLE olist.order_items (
  order_id        varchar(50) NOT NULL,
  order_item_id   int NOT NULL,
  product_id      varchar(50) NULL,
  seller_id       varchar(50) NULL,
  shipping_limit_date datetime2(0) NULL,
  price           decimal(12,2) NULL,
  freight_value   decimal(12,2) NULL,
  CONSTRAINT PK_order_items PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE olist.customers (
  customer_id             varchar(50) NOT NULL,
  customer_unique_id      varchar(50) NULL,
  customer_zip_code_prefix int NULL,
  customer_city           varchar(80) NULL,
  customer_state          char(2) NULL,
  CONSTRAINT PK_customers PRIMARY KEY (customer_id)
);

CREATE TABLE olist.products (
  product_id              varchar(50) NOT NULL,
  product_category_name   varchar(80) NULL,
  product_weight_g        int NULL,
  product_length_cm       int NULL,
  product_height_cm       int NULL,
  product_width_cm        int NULL,
  CONSTRAINT PK_products PRIMARY KEY (product_id)
);
GO
