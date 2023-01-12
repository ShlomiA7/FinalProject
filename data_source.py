import psycopg2
import logging
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger()

ADD_NEW_CLIENT = """insert into client(phone_number, name) values(%s, %s)"""

ADD_NEW_ORDER = """insert into order_(order_number, shipping, client_number, delivery_phone_number)
                    values(%s, %s, %s, %s)"""

ADD_NEW_DISH_IN_ORDER = """insert into dish_in_order(order_number, dish_number, quantity)
                            values(%s, %s, %s)"""

DELETE_DISH_FROM_ORDER = """delete from dish_in_order where dish_number = %s and order_number = %s"""

SELECT_DISHES = """SELECT * FROM dish where dish_type like %s"""

GET_LAST_ORDER_NUMBER = """select max(order_number) from order_"""

GET_DISH_NUMBER = """select dish_number from dish where dish_name = %s"""

GET_SUM_PRICE = """select sum(d.price * dio.quantity)
                    from dish_in_order dio join dish d on dio.dish_number = d.dish_number 
                    where dio.order_number = %s"""

GET_CLIENTS_DATA = """select o.client_number, sum(d.price) / count(dio.*) as AVG_price,
                        sum(cast(cast(d.chiken as int) as numeric)) / count(dio.*) as chicken,
                        sum(cast(cast(d.spicy as int) as numeric)) / count(dio.*) as spicy,
                        sum(cast(cast(d.pastry as int) as numeric)) / count(dio.*) as pastry,
                        sum(cast(cast(d.fish as int) as numeric)) / count(dio.*) as fish,
                        sum(cast(cast(d.tofu as int) as numeric)) / count(dio.*) as tofu,
                        sum(cast(cast(d.beef as int) as numeric)) / count(dio.*) as beef,
                        sum(cast(cast(d.rice as int) as numeric)) / count(dio.*) as rice,
                        sum(cast(cast(d.coconut_cream as int) as numeric)) / count(dio.*) as coconut_cream,
                        sum(cast(cast(d.eggs as int) as numeric)) / count(dio.*) as eggs,
                        sum(cast(cast(d.sea_food as int) as numeric)) / count(dio.*) as sea_food,
                        sum(cast(cast(d.curry as int) as numeric)) / count(dio.*) as curry,
                        sum(cast(cast(d.fried as int) as numeric)) / count(dio.*) as fried,
                        sum(cast(cast(d.vegetarian as int) as numeric)) / count(dio.*) as vegetarian,
                        sum(cast(cast(d.vegan as int) as numeric)) / count(dio.*) as vegan
                        from order_ o join dish_in_order dio on o.order_number = dio.order_number
                                        join dish d on d.dish_number = dio.dish_number
                        group by o.client_number"""

GET_RECOMMENDED_DISHES = """ select q.dish_name, q.price
                            from (select o1.client_number,d1.dish_name, d1.price,
                             cast(count(d1.dish_name) as numeric) /
                            cast((select count(*) from order_ o2 where 
                             o2.client_number = o1.client_number) as numeric) as rating
                              from dish_in_order dio1 join dish d1 
                              on dio1.dish_number = d1.dish_number 
                              join order_ o1 on o1.order_number = dio1.order_number
                              where o1.client_number = %s 
                             or o1.client_number = %s
                             or o1.client_number = %s 
                             or o1.client_number = %s
                             group by o1.client_number,d1.dish_name, d1.price) q 
                             group by q.dish_name, q.price
                             order by sum(q.rating) desc 
                             LIMIT 5"""

GET_DISHES_CURRENT_ORDER = """select d.dish_name, dio.quantity from dish d join dish_in_order dio on d.dish_number
                                = dio.dish_number where dio.order_number = %s"""

IS_CLIENT_NEW = """select client_number from order_ where client_number = %s"""

GET_FAVORITE = """ select q.dish_name, q.price
                            from (select o1.client_number,d1.dish_name, d1.price,
                             cast(count(d1.dish_name) as numeric) /
                            cast((select count(*) from order_ o2 where 
                             o2.client_number = o1.client_number) as numeric) as rating
                              from dish_in_order dio1 join dish d1 
                              on dio1.dish_number = d1.dish_number 
                              join order_ o1 on o1.order_number = dio1.order_number
                             group by o1.client_number,d1.dish_name, d1.price) q 
                             group by q.dish_name, q.price
                             order by sum(q.rating) desc 
                             LIMIT 5"""

GET_INCOME = """select  o.order_time, sum(d.price * dio.quantity) as daily_income
                    from order_ o join dish_in_order dio on o.order_number = dio.order_number
                        join dish d on d.dish_number = dio.dish_number 
                    WHERE o.order_time BETWEEN current_date - INTERVAL '1 week' AND current_date  
                    group by o.order_time"""

GET_DELIVERY_PERSON = """select d.name, d.phone_number
                            from delivery_person d join order_ o on d.phone_number = o.delivery_phone_number
                            where o.order_number = %s"""

GET_LESS_SEAL_DISHES = """select d.dish_name, sum(dio.quantity), sum(dio.quantity * d.price)
                            from dish d join dish_in_order dio on d.dish_number = dio.dish_number
                                join order_ o on o.order_number = dio.order_number
                            where date_part('month',o.order_time) = date_part('month', CURRENT_DATE) 
                                and date_part('year',o.order_time) = date_part('year', CURRENT_DATE)
                            group by d.dish_name
                            order by sum(dio.quantity * d.price)  
                            limit 5"""

GET_BEST_SELLERS = """select d.dish_name, sum(dio.quantity), sum(dio.quantity * d.price)
                        from dish d join dish_in_order dio on d.dish_number = dio.dish_number
                            join order_ o on o.order_number = dio.order_number
                        where date_part('month',o.order_time) = date_part('month', CURRENT_DATE) 
                            and date_part('year',o.order_time) = date_part('year', CURRENT_DATE)
                        group by d.dish_name
                        order by sum(dio.quantity * d.price)  desc
                        limit 5"""

GET_DISH_TYPE_INCOME = """select d.dish_type, sum(d.price * dio.quantity) as income
                            from dish d join dish_in_order dio on d.dish_number = dio.dish_number 
                            group by d.dish_type"""

SET_REMARKS = """update order_ set remarks = %s where order_number = %s"""

GET_REMARK = """select remarks from order_ where order_number = %s"""

class DataSource:
    def __init__(self, database_url):
        self.database_url = database_url

    def get_connection(self):
        return psycopg2.connect(self.database_url, sslmode='allow')

    @staticmethod
    def close_connection(conn):
        if conn is not None:
            conn.close()

    def new_row(self, query, *args):
        conn = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(query, args)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)

    def new_client(self, phone_number, name):
        self.new_row(ADD_NEW_CLIENT, phone_number, name)

    def new_order(self, order_number, shipping, client_number, delivery_phone_number):
        self.new_row(ADD_NEW_ORDER, order_number, shipping, client_number, delivery_phone_number)

    def new_dish_in_order(self, order_number, dish_number, quantity):
        self.new_row(ADD_NEW_DISH_IN_ORDER, order_number, dish_number, quantity)

    def delete_dish_from_order(self, dish_number, order_number):
        self.new_row(DELETE_DISH_FROM_ORDER, dish_number, order_number)

    def set_remarks(self, remarks, order_number):
        self.new_row(SET_REMARKS, remarks, order_number)

    def get_last_order(self):
        conn = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_LAST_ORDER_NUMBER)
            number = cur.fetchall()[0]
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return number[0]

    def get_dishes(self, dish_type):
        conn = None
        dishes = list()
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(SELECT_DISHES, (dish_type,))
            for row in cur.fetchall():
                add_dish = row[1] + "\t" + str(row[2]) + "₪"
                dishes.append(add_dish)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return dishes

    def get_dish_number(self, dish_name):
        conn = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_DISH_NUMBER, (dish_name,))
            dish_number = cur.fetchall()[0]
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return dish_number[0]

    def get_sum_price(self, order_number):
        conn = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_SUM_PRICE, (order_number,))
            sum_price = cur.fetchall()[0]
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return sum_price[0]

    def get_clients_taste_df(self):
        conn = None
        try:
            conn = self.get_connection()
            sql_query = pd.read_sql_query(GET_CLIENTS_DATA, conn)
            df = pd.DataFrame(sql_query,
                              columns=['client_number', 'avg_price', 'chicken', 'spicy', 'pastry', 'fish', 'tofu',
                                       'beef', 'rice', 'coconut_cream', 'eggs', 'sea_food', 'curry', 'fried',
                                       'vegetarian', 'vegan'])
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            data_f = df.pivot_table(index='client_number')
            return data_f

    def get_recommendation_dishes(self, user):
        df = self.get_clients_taste_df()
        normal_df = self.normalize(df)
        similarity_df = self.get_similarity(normal_df)
        close_users = similarity_df.sort_values(by=[user], ascending=False)
        top_close = list(close_users.head(5).index)
        top_close = top_close[1:]
        recommendation_dishes = self.get_top_recommended_dishes(top_close[0], top_close[1], top_close[2], top_close[3])
        return recommendation_dishes

    def normalize(self, df_min_max_scaled):
        columns = list(df_min_max_scaled.columns)
        for column in columns:
            df_min_max_scaled[column] = (df_min_max_scaled[column] - df_min_max_scaled[column].min()) / (
                    df_min_max_scaled[column].max()
                    - df_min_max_scaled[column].min())
        return df_min_max_scaled

    def get_similarity(self, normal_df):
        similarity = cosine_similarity(normal_df)
        similarity_df = pd.DataFrame(similarity)
        similarity_df.index = normal_df.index
        similarity_df.columns = normal_df.index
        return similarity_df

    def get_top_recommended_dishes(self, client1, client2, client3, client4):
        conn = None
        dishes = list()
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_RECOMMENDED_DISHES, (client1, client2, client3, client4,))
            for row in cur.fetchall():
                add_dish = row[0] + "\t" + str(row[1]) + "₪"
                dishes.append(add_dish)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return dishes

    def get_current_dishes(self, order_number):
        conn = None
        dishes = list()
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_DISHES_CURRENT_ORDER, (order_number,))
            for row in cur.fetchall():
                dishes.append(row)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            out = [item for dish in dishes for item in dish]
            return out

    def is_client_new(self, client_number):
        conn = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(IS_CLIENT_NEW, (client_number,))
            is_new = len(cur.fetchall()) < 2
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return is_new

    def get_favorite_dishes(self):
        conn = None
        dishes = list()
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_FAVORITE)
            for row in cur.fetchall():
                add_dish = row[0] + "\t" + str(row[1]) + "₪"
                dishes.append(add_dish)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return dishes

    def get_income_df(self, query, col1, col2):
        conn = None
        try:
            conn = self.get_connection()
            sql_query = pd.read_sql_query(query, conn)
            df = pd.DataFrame(sql_query,
                              columns=[col1, col2])
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return df

    def get_last_week_income_df(self):
        return self.get_income_df(GET_INCOME, 'order_time', 'daily_income')

    def get_dish_type_income_df(self):
        return self.get_income_df(GET_DISH_TYPE_INCOME, 'dish_type', 'income')

    def get_delivery_person(self, order_number):
        conn = None
        dishes = list()
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_DELIVERY_PERSON, (order_number,))
            for row in cur.fetchall():
                dishes.append(row)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            out = [item for dish in dishes for item in dish]
            return out

    def get_seal_dishes(self, query):
        conn = None
        dishes = list()
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(query)
            for row in cur.fetchall():
                add_dish = row[0] + "\t\tsold\t" + str(row[1]) + "\t\tand generated\t" + str(row[2]) + " ₪"
                dishes.append(add_dish)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            return dishes

    def get_less_seal_dishes(self):
        return self.get_seal_dishes(GET_LESS_SEAL_DISHES)

    def get_best_seal_dishes(self):
        return self.get_seal_dishes(GET_BEST_SELLERS)

    def get_remark(self, order_number):
        conn = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(GET_REMARK, (order_number,))
            remark = cur.fetchall()
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            self.close_connection(conn)
            temp = remark[0]
            remark = temp[0]
            return remark

