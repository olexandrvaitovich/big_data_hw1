
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, dict_factory
import cassandra

cluster = Cluster(['172.31.86.170', '172.31.80.71', '172.31.80.145'])
session = cluster.connect('big_date')
session.row_factory = dict_factory

from flask import Flask, jsonify, request, abort, Response
app = Flask(__name__)


@app.route('/<string:table>/<string:id>/', methods=["GET"])
def get_all(table, id):
    rows = session.execute("select * from {} where {}_id='{}' and review_date>'1900-01-01' allow filtering".format(table, table[::-1][:table[::-1].index('_')][::-1], id))
    res = []
    for i in rows:
        temp = dict(i)
        temp['review_date'] = str(temp['review_date'])
        res.append(temp)
    return jsonify(res)


@app.route('/reviews_by_product/<string:id>/<string:rating>/', methods=['GET'])
def get_all_by_rating(id, rating):
    rows = session.execute("select * from reviews_by_product where product_id='{}' and star_rating='{}' and review_date>'1900-09-09' allow filtering".format(id, rating))
    res = []
    for i in rows:
        temp = dict(i)
        temp['review_date'] = str(temp['review_date'])
        res.append(temp)
    return jsonify(res)



@app.route('/reviews_by_product/<int:num>/<string:beg>/<string:end>/')
def get_products(num, beg, end):
    rows = session.execute("select marketplace, product_id, product_parent, product_title, product_category, vine from reviews_by_product where review_date>='{}' and review_date<='{}' allow filtering".format(beg, end))
    products= dict()
    product_ids = dict()
    for i in rows:
        temp = dict(i)
        if temp['product_id'] not in products:
            products[temp['product_id']] = temp
            product_ids[temp['product_id']] = 1 
        else:
            product_ids[temp['product_id']] += 1
    res = list({k: v for k, v in sorted(product_ids.items(), key=lambda item:item[1])}.keys())[-num:]
    return jsonify([products[i] for i in res])


@app.route('/reviews_by_product/frac/<int:num>/<string:beg>/<string:end>/')
def get_products_by_frac(num, beg, end):
    rows = session.execute("select marketplace, product_id, product_parent, product_title, product_category, star_rating, vine, verified_purchase from reviews_by_product where review_date>='{}' and review_date<='{}' allow filtering".format(beg, end))
    products = dict()
    product_ids = dict()
    for i in rows:
        temp = dict(i)
        if temp['product_id'] not in products:
            products[temp['product_id']] = temp
            if temp['star_rating']=='5.0':
                product_ids[temp['product_id']] = [1,1,0]
            else:
                product_ids[temp['product_id']] = [1,0,0]
        else:
            product_ids[temp['product_id']][0]+=1
            if temp['star_rating']=='5.0':
                product_ids[temp['product_id']][1]+=1
        if temp['verified_purchase']=='Y':
            product_ids[temp['product_id']][2]+=1
    res = list({k:v for k, v in sorted(product_ids.items(), key=lambda item:(item[1][1]/item[1][0]) if item[1][2]>=100 else 0)}.keys())[-num:]
    return jsonify([products[i] for i in res])


@app.route('/reviews_by_customer/<int:num>/<string:beg>/<string:end>/')
def get_most_productive_customers(num, beg, end):
    rows = session.execute("select marketplace, customer_id, vine, verified_purchase from reviews_by_customer where review_date>='{}' and review_date<='{}' allow filtering".format(beg, end))
    customers = dict()
    customer_ids = dict()
    for i in rows:
        temp = dict(i)
        if temp['customer_id'] not in customers:
            print(1)
            customers[temp['customer_id']] = temp
            customer_ids[temp['customer_id']] = 0 if temp['verified_purchase']=='N' else 1
        else:
            customer_ids[temp['customer_id']]+=1 if temp['verified_purchase']=='Y' else 0
    res = list({k: v for k, v in sorted(customer_ids.items(), key=lambda item: item[1])}.keys())[-num:]
    return jsonify([customers[i] for i in res])


@app.route('/reviews_by_customer/<int:num>/<string:beg>/<string:end>/<string:rating>/')
def get_most_productive_star_customer(num, beg, end, rating):
    rows = session.execute("select marketplace, customer_id, vine, star_rating from reviews_by_customer where review_date>='{}' and review_date<='{}' allow filtering".format(beg, end))
    customers = dict()
    customer_ids = dict()
    for i in rows:
        temp = dict(i)
        if temp['customer_id'] not in customers:
            customers[temp['customer_id']] = temp
            customer_ids[temp['customer_id']] = 0
        if (float(temp['star_rating'])<3 and rating=='low') or (float(temp['star_rating'])>3 and rating=='high'):
            customer_ids[temp['customer_id']]+=1
    res = list({k: v for k, v in sorted(customer_ids.items(), key=lambda item: item[1])}.keys())[-num:]
    return jsonify([customers[i] for i in res])




if __name__ == '__main__':
	app.run(host='ec2-52-207-219-107.compute-1.amazonaws.com')
