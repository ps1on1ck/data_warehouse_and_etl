CREATE SCHEMA IF NOT EXISTS core;

DROP TABLE IF EXISTS core.h_orders;
CREATE TABLE core.h_orders (
	 h_order_rk SERIAL PRIMARY KEY,
	 order_id int4 NOT NULL,
	 source_system varchar,
	 processed_dttm date,
	 order_bk varchar,
	 UNIQUE(order_bk)
);

DROP TABLE IF EXISTS core.s_orders;
CREATE TABLE core.s_orders (
	 h_order_rk int NOT NULL,
	 order_date date NULL,
	 order_status varchar(1) NULL,
	 order_priority varchar(15) NULL,
	 clerk varchar(15) NULL,
	 source_system varchar,
	 valid_from_dttm date,
	 valid_to_dttm date,
	 processed_dttm date,
	 CONSTRAINT s_orders_pk PRIMARY KEY (h_order_rk),
	 CONSTRAINT s_orders_fk FOREIGN KEY (h_order_rk) REFERENCES core.h_orders(h_order_rk)
);

DROP TABLE IF EXISTS core.h_products;
CREATE TABLE core.h_products (
	 h_product_rk SERIAL PRIMARY KEY,
	 product_id int4 NOT NULL,
	 source_system varchar,
	 processed_dttm date,
	 product_bk varchar,
	 UNIQUE(product_bk)
);

DROP TABLE IF EXISTS core.s_products;
CREATE TABLE core.s_products (
	 h_product_rk int NOT NULL,
	 product_name varchar(55) NULL,
	 product_type varchar(25) NULL,
	 product_size int4 NULL,
	 retail_price numeric(15,2) NULL,
	 source_system varchar,
	 valid_from_dttm date,
	 valid_to_dttm date,
	 processed_dttm date,
	 CONSTRAINT s_products_pk PRIMARY KEY (h_product_rk),
	 CONSTRAINT s_products_fk FOREIGN KEY (h_product_rk) REFERENCES core.h_products(h_product_rk)
);

DROP TABLE IF EXISTS core.h_suppliers;
CREATE TABLE core.h_suppliers (
	 h_supplier_rk SERIAL PRIMARY KEY,
	 supplier_id int4 NOT NULL,
	 source_system varchar,
	 processed_dttm date,
	 supplier_bk varchar,
	 UNIQUE(supplier_bk)
);

DROP TABLE IF EXISTS core.s_suppliers;
CREATE TABLE core.s_suppliers (
	 h_supplier_rk int NOT NULL,
	 supplier_name varchar(25) NULL,
	 address varchar(40) NULL,
	 phone varchar(15) NULL,
	 balance numeric(15,2) NULL,
	 descr varchar(101) NULL,
	 source_system varchar,
	 valid_from_dttm date,
	 valid_to_dttm date,
	 processed_dttm date,
	 CONSTRAINT s_suppliers_pk PRIMARY KEY (h_supplier_rk),
	 CONSTRAINT s_suppliers_fk FOREIGN KEY (h_supplier_rk) REFERENCES core.h_suppliers(h_supplier_rk)
);

DROP TABLE IF EXISTS core.l_order_details;
CREATE TABLE core.l_order_details (
     l_order_detail_rk SERIAL UNIQUE,
	 h_order_rk int NOT NULL,
	 h_product_rk int NOT NULL,
	 source_system varchar,
	 processed_dttm date,
	 order_detail_bk varchar,
	 UNIQUE(order_detail_bk),
	 CONSTRAINT l_order_details_pk PRIMARY KEY (l_order_detail_rk, h_order_rk, h_product_rk),
	 CONSTRAINT l_order_details_fk FOREIGN KEY (h_order_rk) REFERENCES core.h_orders(h_order_rk),
	 CONSTRAINT l_order_details_fk_1 FOREIGN KEY (h_product_rk) REFERENCES core.h_products(h_product_rk)
);

DROP TABLE IF EXISTS core.s_l_order_details;
CREATE TABLE core.s_l_order_details (
	 l_order_detail_rk int NOT NULL,
	 unit_price numeric(15,2) NULL,
	 quantity numeric(15,2) NULL,
	 discount numeric(15,2) NULL,
	 source_system varchar,
	 valid_from_dttm date,
	 valid_to_dttm date,
	 processed_dttm date,
	 CONSTRAINT s_l_order_details_pk PRIMARY KEY (l_order_detail_rk),
	 CONSTRAINT s_l_order_details_fk FOREIGN KEY (l_order_detail_rk) REFERENCES core.l_order_details(l_order_detail_rk)
);


DROP TABLE IF EXISTS core.l_product_suppl;
CREATE TABLE core.l_product_suppl (
     l_product_suppl_rk SERIAL UNIQUE,
	 h_supplier_rk int NOT NULL,
	 h_product_rk int NOT NULL,
	 source_system varchar,
	 processed_dttm date,
	 product_suppl_bk varchar,
	 UNIQUE(product_suppl_bk),
	 CONSTRAINT l_product_suppl_pk PRIMARY KEY (l_product_suppl_rk, h_supplier_rk, h_product_rk),
	 CONSTRAINT l_product_suppl_fk FOREIGN KEY (h_supplier_rk) REFERENCES core.h_suppliers(h_supplier_rk),
	 CONSTRAINT l_product_suppl_fk_1 FOREIGN KEY (h_product_rk) REFERENCES core.h_products(h_product_rk)
);

DROP TABLE IF EXISTS core.s_l_product_suppl;
CREATE TABLE core.s_l_product_suppl (
	 l_product_suppl_rk int NOT NULL,
	 qty int4 NULL,
	 supply_cost numeric(15,2) NULL,
	 descr varchar(199) NULL,
	 source_system varchar,
	 valid_from_dttm date,
	 valid_to_dttm date,
	 processed_dttm date,
	 CONSTRAINT s_l_product_suppl_pk PRIMARY KEY (l_product_suppl_rk),
	 CONSTRAINT s_l_product_suppl_fk FOREIGN KEY (l_product_suppl_rk) REFERENCES core.l_product_suppl(l_product_suppl_rk)
);

