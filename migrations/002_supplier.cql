ALTER TABLE warehouse.inventory_by_product_zone ADD supplier_id text;
ALTER TABLE warehouse.inventory_by_zone ADD supplier_id text;
ALTER TABLE warehouse.event_history ADD supplier_id text;
