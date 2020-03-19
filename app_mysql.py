from sqlalchemy import create_engine, func, case, or_
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker 
import sqlalchemy as db



db_url = {
    'database': 'big_data',
    'drivername': 'mysql',
    'username': 'root',
    'password': '123321',
    'host': '127.0.0.1',
    'query': {'charset': 'utf8mb4'},
}

engine = create_engine(URL(**db_url), encoding="utf8")

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Text, Date
metadata = MetaData()
reviews_table = Table('reviews', metadata, 
                      Column('customer_id', Text), 
                      Column('review_id', String(255), primary_key=True), 
                      Column('product_id', Text), 
                      Column('star_rating', Text),
                      Column('helpful_votes', Text),
                      Column('total_votes', Text),
                      Column('vine', Text),
                      Column('verified_purchase', Text),
                      Column('review_headline', Text),
                      Column('review_body', Text),
                      Column('review_date', Date))

customer_table = Table('customer', metadata, 
                      Column('marketplace', Text), 
                      Column('customer_id', String(255), primary_key=True))

product_table = Table('product', metadata, 
                      Column('product_id', String(255), primary_key=True), 
                      Column('product_parent', Text),
                      Column('product_title', Text),
                      Column('product_category', Text))

class Review:    
    def __init__(self, review_id, customer_id, product_id,\
             star_rating, helpful_votes,\
             total_votes, vine, verified_purchase, review_headline, review_body, review_date,):
        self.customer_id = customer_id
        self.review_id = review_id
        self.product_id = product_id
        self.star_rating = star_rating
        self.helpful_votes = helpful_votes
        self.total_votes = total_votes
        self.vine = vine
        self.verified_purchase = verified_purchase
        self.review_headline = review_headline
        self.review_body = review_body
        self.review_date = review_date

    def __repr__(self):
        return "<User('%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s')>" % (
                self.review_id,
                self.customer_id,
                self.product_id,
                self.star_rating,
                self.helpful_votes,
                self.total_votes,
                self.vine,
                self.verified_purchase,
                self.review_headline,
                self.review_body,
                self.review_date)
    

class Customer:    
    def __init__(self, customer_id, marketplace):
        self.marketplace = marketplace
        self.customer_id = customer_id

    def __repr__(self):
        return "<User('%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s')>" % (
                self.customer_id,    
                self.marketplace)
    
    
class Product:    
    def __init__(self, product_id, product_parent, product_title, product_category):
        self.product_id = product_id
        self.product_parent = product_parent
        self.product_title = product_title
        self.product_category = product_category

    def __repr__(self):
        return "<User('%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s')>" % (
                self.product_id,
                self.product_parent,
                self.product_title,
                self.product_category)


mapper(Review, reviews_table)
mapper(Product, product_table)
mapper(Customer, customer_table)

metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

from flask import Flask, jsonify, request, abort, Response
app = Flask(__name__)


@app.route('/<string:table>/<string:id>/', methods=["GET"])
def get_all(table, id):
    col = {'customer':Review.customer_id, 'product':Review.product_id}
    query = session.execute(session.query(Review).filter(col[table]==id))
    result = [{k:v for k, v in zip(reviews_table.columns.keys(), i)} for i in list(query)]
    for i in range(len(result)):
        result[i]['review_date'] = str(result[i]['review_date'])
    return jsonify(result)


@app.route('/product/<string:id>/<string:rating>/', methods=['GET'])
def get_all_by_rating(id, rating):
    query = session.execute(session.query(Review).filter(Review.product_id==id, Review.star_rating==rating))
    result = [{k:v for k, v in zip(reviews_table.columns.keys(), i)} for i in list(query)]
    for i in range(len(result)):
        result[i]['review_date'] = str(result[i]['review_date'])
    return jsonify(result)


@app.route('/product/<int:num>/<string:beg>/<string:end>/')
def get_products(num, beg, end):
    query = session.execute(session.query(Review, Product).filter(Review.product_id==Product.product_id).filter(Review.review_date.between(beg, end)).group_by(Product.product_id).order_by(func.count(Review.product_id).desc()).limit(num))
    result = [{k:v for k, v in zip(reviews_table.columns.keys(), i)} for i in list(query)]
    for i in range(len(result)):
        result[i]['review_date'] = str(result[i]['review_date'])
    return jsonify(result)


@app.route('/product/frac/<int:num>/<string:beg>/<string:end>/')
def get_products_by_frac(num, beg, end):
    case_s = case([(Review.star_rating=='5.0', 1), (Review.star_rating!='5.0', 0)])
    query = session.execute(session.query(Review, Product).filter(Review.product_id==Product.product_id).filter(Review.review_date.between(beg, end)).group_by(Product.product_id).order_by((func.sum(case_p)/func.count(Review.product_id)).desc()).limit(num))
    result = [{k:v for k, v in zip(reviews_table.columns.keys(), i)} for i in list(query)]
    for i in range(len(result)):
        result[i]['review_date'] = str(result[i]['review_date'])
    return jsonify(result)

@app.route('/customer/<int:num>/<string:beg>/<string:end>/')
def get_most_productive_customers(num, beg, end):
    query = session.execute(session.query(Review, Customer).filter(Review.customer_id==Customer.customer_id).filter(Review.review_date.between(beg, end)).filter(Review.verified_purchase=='Y').group_by(Customer.customer_id).order_by(func.count(Review.customer_id).desc()).limit(num))
    result = [{k:v for k, v in zip(reviews_table.columns.keys(), i)} for i in list(query)]
    for i in range(len(result)):
        result[i]['review_date'] = str(result[i]['review_date'])
    return jsonify(result)


@app.route('/reviews_by_customer/<int:num>/<string:beg>/<string:end>/<string:rating>/')
def get_most_productive_star_customer(num, beg, end, rating):
    stars = ['1.0', '2.0'] if rating=='low' else ['4.0', '5.0']
    query = session.execute(session.query(Review, Customer).filter(Review.customer_id==Customer.customer_id).filter(Review.review_date.between(beg, end)).filter(or_(Review.star_rating==stars[0], Review.star_rating==stars[1])).group_by(Customer.customer_id).order_by(func.count(Review.customer_id).desc()).limit(num))
    result = [{k:v for k, v in zip(reviews_table.columns.keys(), i)} for i in list(query)]
    for i in range(len(result)):
        result[i]['review_date'] = str(result[i]['review_date'])
    return jsonify(result)




if __name__ == '__main__':
    app.run()#host='ec2-52-207-219-107.compute-1.amazonaws.com')
