import grpc
from concurrent import futures
import uuid
from threading import Lock

import product_pb2
import product_pb2_grpc

class ProductService(product_pb2_grpc.ProductServiceServicer):
    def __init__(self):
        self.products = {}
        self.lock = Lock()
        self._add_sample_products()
    
    def _add_sample_products(self):
        sample_products = [
            {
                "product_id": "1",
                "name": "Laptop",
                "description": "High-performance laptop",
                "price": 999.99,
                "stock": 10,
                "category": "Electronics",
                "image_url": "https://example.com/laptop.jpg"
            },
            {
                "product_id": "2",
                "name": "Smartphone",
                "description": "Latest smartphone model",
                "price": 699.99,
                "stock": 25,
                "category": "Electronics",
                "image_url": "https://example.com/phone.jpg"
            },
            {
                "product_id": "3",
                "name": "Headphones",
                "description": "Wireless noise-cancelling headphones",
                "price": 199.99,
                "stock": 50,
                "category": "Electronics",
                "image_url": "https://example.com/headphones.jpg"
            }
        ]
        
        for product in sample_products:
            self.products[product["product_id"]] = product
    
    def GetProduct(self, request, context):
        product = self.products.get(request.product_id)
        if product:
            return product_pb2.ProductResponse(
                product_id=product["product_id"],
                name=product["name"],
                description=product["description"],
                price=product["price"],
                stock=product["stock"],
                category=product["category"],
                image_url=product["image_url"],
                status="success",
                message="Product found"
            )
        else:
            return product_pb2.ProductResponse(
                status="error",
                message="Product not found"
            )
    
    def ListProducts(self, request, context):
        page = request.page if request.page > 0 else 1
        limit = request.limit if request.limit > 0 else 10
        category = request.category
        
        filtered_products = []
        for product in self.products.values():
            if not category or product["category"] == category:
                filtered_products.append(product)
        
        # Pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_products = filtered_products[start_idx:end_idx]
        
        product_responses = []
        for product in paginated_products:
            product_responses.append(product_pb2.ProductResponse(
                product_id=product["product_id"],
                name=product["name"],
                description=product["description"],
                price=product["price"],
                stock=product["stock"],
                category=product["category"],
                image_url=product["image_url"]
            ))
        
        return product_pb2.ProductListResponse(
            products=product_responses,
            total=len(filtered_products),
            page=page,
            limit=limit
        )
    
    def CreateProduct(self, request, context):
        with self.lock:
            product_id = str(uuid.uuid4())
            new_product = {
                "product_id": product_id,
                "name": request.name,
                "description": request.description,
                "price": request.price,
                "stock": request.stock,
                "category": request.category,
                "image_url": request.image_url
            }
            
            self.products[product_id] = new_product
            
            return product_pb2.ProductResponse(
                product_id=product_id,
                name=request.name,
                description=request.description,
                price=request.price,
                stock=request.stock,
                category=request.category,
                image_url=request.image_url,
                status="success",
                message="Product created successfully"
            )
    
    def UpdateProduct(self, request, context):
        with self.lock:
            product = self.products.get(request.product_id)
            if product:
                if request.name:
                    product["name"] = request.name
                if request.description:
                    product["description"] = request.description
                if request.price > 0:
                    product["price"] = request.price
                if request.stock >= 0:
                    product["stock"] = request.stock
                if request.category:
                    product["category"] = request.category
                if request.image_url:
                    product["image_url"] = request.image_url
                
                return product_pb2.ProductResponse(
                    product_id=product["product_id"],
                    name=product["name"],
                    description=product["description"],
                    price=product["price"],
                    stock=product["stock"],
                    category=product["category"],
                    image_url=product["image_url"],
                    status="success",
                    message="Product updated successfully"
                )
            else:
                return product_pb2.ProductResponse(
                    status="error",
                    message="Product not found"
                )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    product_pb2_grpc.add_ProductServiceServicer_to_server(ProductService(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Product Service running on port 50052")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
