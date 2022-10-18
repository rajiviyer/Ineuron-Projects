import json

json_str = "{'order_no': 16117, 'order_date': '03/08/2019 20:17', 'item_name': 'Tandoori Chicken (1/4)', 'quantity': 1, 'product_price': 4.95, 'total_products': 7}"

json_data = eval(json_str)
print(json_data['order_date'])