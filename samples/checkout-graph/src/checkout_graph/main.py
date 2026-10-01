from ankka_flow import serve

from .mapper import CheckoutGraph

if __name__ == "__main__":
    serve(CheckoutGraph())
