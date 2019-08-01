SELECT orders.Order AS
ORDER Number, orders.Part,
              orders.CustomerID_1,
              sum(orders.Value) AS sum_Value,
              CASE
                  WHEN CustomerID_1 <> NULL THEN CustomerID_1
                  ELSE CustomerID_2
              END AS CustomerID
FROM orders_schema.orders
GROUP BY orders.Order,
         orders.Part,
         orders.CustomerID_1,
         orders.CustomerID_2