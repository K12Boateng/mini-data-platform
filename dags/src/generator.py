# generate_sales_data.py
import pandas as pd
import uuid
import random
from datetime import datetime, timedelta

def generate_sales_data(n: int = 1000, output_file: str = "sales.csv"):
    """
    Generate synthetic sales data that passes validation.
    Produces a CSV with >= n records.
    """
    records = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 1, 1)

    for _ in range(n):
        sale_id = str(uuid.uuid4())  # unique ID
        sale_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        customer_id = f"CUST-{random.randint(1000, 9999)}"
        product_id = f"PROD-{random.randint(100, 999)}"
        quantity = random.randint(1, 20)
        amount = round(random.uniform(10, 500) * quantity, 2)

        records.append(
            {
                "sale_id": sale_id,
                "sale_date": sale_date.strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "amount": amount,
            }
        )

    df = pd.DataFrame(records)
    df.to_csv(output_file, index=False)
    print(f" Generated {len(df)} records into {output_file}")


if __name__ == "__main__":
    generate_sales_data(n=1200)  # default: 1200 records
