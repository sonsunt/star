import pathlib
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class PreTransformation(ABC):
    filepath: pathlib.Path
    data: pd.DataFrame = None

    @abstractmethod
    def transform(self):
        pass
    
    # @abstractmethod
    # def validate(df):
    #     pass

    @abstractmethod
    def export_csv(self, output_folder: pathlib.Path) -> None:
        pass

class CustomerTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self) -> pd.DataFrame:
        cust_df = pd.read_csv(self.filepath,
                      dtype={
                          "customer_id": str,
                          "customer_unique_id": str,
                          "customer_zip_code_prefix": str,
                          "customer_city": str,
                          "customer_state": str
                          })
        self.data = cust_df

    def validate(self) -> bool:
        if self.data is None:
            raise ValueError("No data to validate.")
        test_cust_zip_prefix_not_null = self.data["customer_zip_code_prefix"].notnull().all()
        test_cust_zip_prefix_five_digits = (self.data["customer_zip_code_prefix"].str.len() == 5).all()
        test_cust_state_not_null = self.data["customer_state"].notnull().all()
        test_cust_state_brazil_state = self.data["customer_state"].str.contains()
        
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "customers_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)
    
class GeoTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self) -> pd.DataFrame:
        geo_df = pd.read_csv(self.filepath,
                            dtype={
                                "geolocation_zip_code_prefix": str,
                                "geolocation_lat": float,
                                "geolocation_lng": float,
                                "geolocation_city": str,
                                "geolocation_state": str
                            })
        self.data = geo_df

    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "geolocation_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)
    
class OrderItemsTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self):
        order_item_df = pd.read_csv(self.filepath,
                            dtype={
                            "order_id": str,
                            "order_item_id": int,
                            "product_id": str,
                            "seller_id": str,
                            "price": float,
                            "freight_value": float
                            },
                            parse_dates=["shipping_limit_date"])
        order_item_df = order_item_df.assign(
            total_value=order_item_df["price"] + order_item_df["freight_value"])
        self.data = order_item_df
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "order_items_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)

class OrderPaymentsTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self):
        order_payments_df = pd.read_csv(self.filepath,
                                dtype={
                                    "order_id": str,
                                    "payment_sequential": int,
                                    "payment_type": str,
                                    "payment_installments": int,
                                    "payment_value": float
                                })
        self.data = order_payments_df
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "order_payments_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)

class OrderReviewsTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self):
        order_reviews_df = pd.read_csv(self.filepath,
                               dtype={
                                   "review_id": str,
                                   "order_id": str,
                                   "review_score": int,
                                   "review_comment_title": str,
                                   "review_comment_message": str,
                                   "satisfaction": str
                               },
                               parse_dates=[
                                   "review_creation_date",
                                   "review_answer_timestamp"
                               ])

        is_happy = order_reviews_df["review_score"] >= 4
        order_reviews_df.loc[is_happy, "satisfaction"] = "happy"
        order_reviews_df.loc[~is_happy, "satisfaction"] = "not happy"
        self.data = order_reviews_df
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "order_reviews_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)
    
class OrdersTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self):
        orders_df = pd.read_csv(self.filepath,
                                        dtype={
                                            "order_id": str,
                                            "customer_id": str,
                                            "order_status": str
                                        },
                                        parse_dates=[
                                            "order_purchase_timestamp",
                                            "order_approved_at",
                                            "order_delivered_carrier_date",
                                            "order_delivered_customer_date",
                                            "order_estimated_delivery_date"
                                        ])
        self.data = orders_df
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "orders_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)

class ProductsTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self):
        products_df = pd.read_csv(self.filepath,
                                header=0,
                                names=[
                                    "product_id",
                                    "product_category_name",
                                    "product_name_length",
                                    "product_description_length",
                                    "product_photos_qty",
                                    "product_weight_g",
                                    "product_length_cm",
                                    "product_height_cm",
                                    "product_width_cm",
                                    ],
                                    dtype={
                                        "product_id": str,
                                        "product_category_name": str,
                                        "product_name_length": float,
                                        "product_description_length": float,
                                        "product_photos_qty": "Int64",
                                        "product_weight_g": float,
                                        "product_length_cm": float,
                                        "product_height_cm": float,
                                        "product_width_cm": float
                                        }
                                    )
        
        products_df = products_df.assign(product_volume_cc=products_df["product_length_cm"] \
                                         * products_df["product_height_cm"] \
                                        * products_df["product_width_cm"])
        
        volumetric_weight = 5000  # Cut off point for "light" and "heavy" packages (cc/kg)
        
        is_heavy = ((products_df["product_volume_cc"]) / (products_df["product_weight_g"] / 1000)) > volumetric_weight
        products_df.loc[is_heavy, "is_heavy"] = "Heavy"
        products_df.loc[~is_heavy, "is_heavy"] = "Light"
        
        self.data = products_df
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "products_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)

class SellersTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self):
        sellers_df = pd.read_csv(self.filepath,
                                dtype={
                                    "seller_id": str,
                                    "seller_zip_code_prefix": str,
                                    "seller_city": str,
                                    "seller_state": str
                                })
        self.data = sellers_df
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "sellers_dataset_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)

class ProductCategoryNameTransformation(PreTransformation):
    def __init__(self, fp: pathlib.Path):
        self.filepath = fp
        self.data = None

    def transform(self):
        en_category_df = pd.read_csv(self.filepath,
                                dtype={
                                    "product_category_name": str,
                                    "product_category_name_english": str
                                })
        self.data = en_category_df
    
    def export_csv(self, output_folder: pathlib.Path) -> None:
        output_filepath = pathlib.Path(output_folder, "product_category_refined.csv")
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(output_filepath, index=False)

output_folder = pathlib.Path("/home/dawson/portfolio-proj-brazil-ecom/refined")

cust = CustomerTransformation(pathlib.Path("../raw/olist_customers_dataset.csv"))
cust.transform()
cust.export_csv(output_folder)

order_items = OrderItemsTransformation(pathlib.Path("../raw/olist_order_items_dataset.csv"))
order_items.transform()
order_items.export_csv(output_folder)

order_payments = OrderPaymentsTransformation(pathlib.Path("../raw/olist_order_payments_dataset.csv"))
order_payments.transform()
order_payments.export_csv(output_folder)

orders = OrdersTransformation(pathlib.Path("../raw/olist_orders_dataset.csv"))
orders.transform()
orders.export_csv(output_folder)

products = ProductsTransformation(pathlib.Path("../raw/olist_products_dataset.csv"))
products.transform()
products.export_csv(output_folder)

sellers = SellersTransformation(pathlib.Path("../raw/olist_sellers_dataset.csv"))
sellers.transform()
sellers.export_csv(output_folder)

product_category_english = ProductCategoryNameTransformation(pathlib.Path("../raw/product_category_name_translation.csv"))
product_category_english.transform()
product_category_english.export_csv(output_folder)