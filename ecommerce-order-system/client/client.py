import grpc
import sys
import os

# Add protos to path
sys.path.append('../protos')
#<<<<<<< HEAD
#=======
sys.path.append('../user_service')
sys.path.append('../product_service')
sys.path.append('../order_service')
sys.path.append('../payment_service')
sys.path.append('../shipping_service')

#>>>>>>> b8d41ee (Servers and client up and running)

import user_pb2
import user_pb2_grpc
import product_pb2
import product_pb2_grpc
import order_pb2
import order_pb2_grpc
import payment_pb2
import payment_pb2_grpc
import shipping_pb2
import shipping_pb2_grpc

class ECommerceClient:
    def __init__(self):
        # self.user_channel = grpc.insecure_channel('user_service:50051')
        # self.product_channel = grpc.insecure_channel('product_service:50052')
        # self.order_channel = grpc.insecure_channel('order_service:50053')
        # self.payment_channel = grpc.insecure_channel('payment_service:50054')
        # self.shipping_channel = grpc.insecure_channel('shipping_service:50055')

        self.user_channel = grpc.insecure_channel('localhost:50051')
        self.product_channel = grpc.insecure_channel('localhost:50052')
        self.order_channel = grpc.insecure_channel('localhost:50053')
        self.payment_channel = grpc.insecure_channel('localhost:50054')
        self.shipping_channel = grpc.insecure_channel('localhost:50055')

        
        self.user_stub = user_pb2_grpc.UserServiceStub(self.user_channel)
        self.product_stub = product_pb2_grpc.ProductServiceStub(self.product_channel)
        self.order_stub = order_pb2_grpc.OrderServiceStub(self.order_channel)
        self.payment_stub = payment_pb2_grpc.PaymentServiceStub(self.payment_channel)
        self.shipping_stub = shipping_pb2_grpc.ShippingServiceStub(self.shipping_channel)
        
        self.current_user = None
    
    def register_user(self):
        print("\n--- User Registration ---")
        username = input("Username: ")
        email = input("Email: ")
        password = input("Password: ")
        first_name = input("First Name: ")
        last_name = input("Last Name: ")
        phone = input("Phone: ")
        address = input("Address: ")
        
        request = user_pb2.UserRegistrationRequest(
            username=username,
            email=email,
            password=password,
            first_name=first_name,
            last_name=last_name,
            phone=phone,
            address=address
        )
        
        try:
            response = self.user_stub.RegisterUser(request)
            if response.status == "success":
                print(f"✅ User registered successfully! User ID: {response.user_id}")
            else:
                print(f"❌ Error: {response.message}")
        except grpc.RpcError as e:
            print(f"❌ gRPC Error: {e.details()}")
    
    def login_user(self):
        print("\n--- User Login ---")
        username = input("Username: ")
        password = input("Password: ")
        
        request = user_pb2.UserLoginRequest(
            username=username,
            password=password
        )
        
        try:
            response = self.user_stub.LoginUser(request)
            if response.status == "success":
                self.current_user = response
                print(f"✅ Login successful! Welcome {response.first_name} {response.last_name}")
            else:
                print(f"❌ Error: {response.message}")
        except grpc.RpcError as e:
            print(f"❌ gRPC Error: {e.details()}")
    
    def browse_products(self):
        print("\n--- Product Catalog ---")
        try:
            request = product_pb2.ProductListRequest(page=1, limit=10)
            response = self.product_stub.ListProducts(request)
            
            for product in response.products:
                print(f"\n📦 Product ID: {product.product_id}")
                print(f"   Name: {product.name}")
                print(f"   Description: {product.description}")
                print(f"   Price: ${product.price:.2f}")
                print(f"   Stock: {product.stock}")
                print(f"   Category: {product.category}")
        except grpc.RpcError as e:
            print(f"❌ gRPC Error: {e.details()}")
    
    def place_order(self):
        if not self.current_user:
            print("❌ Please login first")
            return
        
        print("\n--- Place Order ---")
        self.browse_products()
        
        items = []
        while True:
            product_id = input("\nEnter Product ID to add to cart (or 'done' to finish): ")
            if product_id.lower() == 'done':
                break
            
            try:
                product_request = product_pb2.ProductRequest(product_id=product_id)
                product_response = self.product_stub.GetProduct(product_request)
                
                # if product_response.status == "success":
                quantity = int(input(f"Enter quantity for {product_response.name}: "))
                
                # if quantity > 0 and quantity <= product_response.stock:
                items.append(order_pb2.OrderItem(
                    product_id=product_id,
                    quantity=quantity,
                    price=product_response.price
                ))
                print(f"✅ Added {quantity} x {product_response.name} to cart")
                    # else:
                    #     print("❌ Invalid quantity")
                # else:
                #     print("❌ Product not found")
            except grpc.RpcError as e:
                print(f"❌ gRPC Error: {e.details()}")
        
        if not items:
            print("❌ No items in cart")
            return
        
        shipping_address = input("Shipping Address: ")
        payment_method = input("Payment Method (credit_card/paypal): ")
        
        try:
            # Create order
            order_request = order_pb2.CreateOrderRequest(
                user_id=self.current_user.user_id,
                items=items,
                shipping_address=shipping_address,
                payment_method=payment_method
            )
            order_response = self.order_stub.CreateOrder(order_request)
            
            if order_response.order_id:
                print(f"✅ Order created successfully! Order ID: {order_response.order_id}")
                
                # # Process payment
                # payment_request = payment_pb2.PaymentRequest(
                #     order_id=order_response.order_id,
                #     user_id=self.current_user.user_id,
                #     amount=order_response.total_amount,
                #     payment_method=payment_method,
                #     card_number="4111111111111111",  # Mock card number
                #     expiry_date="12/25",
                #     cvv="123"
                # )
                # payment_response = self.payment_stub.ProcessPayment(payment_request)
                
                # if payment_response.status == "completed":
                #     print(f"✅ Payment processed successfully! Transaction ID: {payment_response.transaction_id}")
                    
                #     # Create shipping
                #     shipping_request = shipping_pb2.ShippingRequest(
                #         order_id=order_response.order_id,
                #         user_id=self.current_user.user_id,
                #         shipping_address=shipping_address,
                #         shipping_method="standard"
                #     )
                #     shipping_response = self.shipping_stub.CreateShipping(shipping_request)
                    
                #     if shipping_response.shipping_id:
                #         print(f"✅ Shipping created successfully! Tracking Number: {shipping_response.tracking_number}")
                        
                #         # Update order status
                #         update_request = order_pb2.UpdateOrderStatusRequest(
                #             order_id=order_response.order_id,
                #             status="confirmed",
                #             admin_id="admin"
                #         )
                #         self.order_stub.UpdateOrderStatus(update_request)
                #         print("✅ Order status updated to 'confirmed'")
                #     else:
                #         print("❌ Failed to create shipping")
                # else:
                #     print(f"❌ Payment failed: {payment_response.message}")
            else:
                print("❌ Failed to create order")
        except grpc.RpcError as e:
            print(f"❌ gRPC Error: {e.details()}")
    
    def track_orders(self):
        if not self.current_user:
            print("❌ Please login first")
            return
        
        print("\n--- Your Orders ---")
        try:
            request = order_pb2.UserOrdersRequest(
                user_id=self.current_user.user_id,
                page=1,
                limit=10
            )
            response = self.order_stub.GetUserOrders(request)
            
            for order in response.orders:
                print(f"\n📦 Order ID: {order.order_id}")
                print(f"   Status: {order.status}")
                print(f"   Total Amount: ${order.total_amount:.2f}")
                print(f"   Created: {order.created_at}")
                print(f"   Items:")
                for item in order.items:
                    print(f"     - Product: {item.product_id}, Qty: {item.quantity}, Price: ${item.price:.2f}")
        except grpc.RpcError as e:
            print(f"❌ gRPC Error: {e.details()}")
    
    def admin_manage_orders(self):
        print("\n--- Admin Order Management ---")
        # In a real system, you'd have proper admin authentication
        
        try:
            # Get all orders (simplified - in real system, you'd have a proper admin endpoint)
            # For demo, we'll just show some admin options
            print("1. Update Order Status")
            print("2. Cancel Order")
            
            choice = input("Enter choice: ")
            
            if choice == "1":
                order_id = input("Order ID: ")
                new_status = input("New Status: ")
                
                request = order_pb2.UpdateOrderStatusRequest(
                    order_id=order_id,
                    status=new_status,
                    admin_id="admin"
                )
                response = self.order_stub.UpdateOrderStatus(request)
                
                if response.order_id:
                    print(f"✅ Order status updated to {new_status}")
                else:
                    print(f"❌ Failed to update order: {response.message}")
            
            elif choice == "2":
                order_id = input("Order ID: ")
                user_id = input("User ID: ")
                
                request = order_pb2.CancelOrderRequest(
                    order_id=order_id,
                    user_id=user_id
                )
                response = self.order_stub.CancelOrder(request)
                
                if response.order_id:
                    print("✅ Order cancelled successfully")
                else:
                    print(f"❌ Failed to cancel order: {response.message}")
        
        except grpc.RpcError as e:
            print(f"❌ gRPC Error: {e.details()}")

def main():
    client = ECommerceClient()
    
    while True:
        print("\n=== E-Commerce Order Management System ===")
        print("1. Register User")
        print("2. Login")
        print("3. Browse Products")
        print("4. Place Order")
        print("5. Track Orders")
        print("6. Admin - Manage Orders")
        print("7. Exit")
        
        choice = input("Enter your choice: ")
        
        if choice == "1":
            client.register_user()
        elif choice == "2":
            client.login_user()
        elif choice == "3":
            client.browse_products()
        elif choice == "4":
            client.place_order()
        elif choice == "5":
            client.track_orders()
        elif choice == "6":
            client.admin_manage_orders()
        elif choice == "7":
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == '__main__':
    main()
