import mysql.connector
from logging_setup import setup_logger
from contextlib import contextmanager


logger = setup_logger('db_helper')

@contextmanager
def get_db_cursor(commit=False):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="expense_manager",
    )
    if connection.is_connected():
        print("Connection Successful")
    else:
        print("Failed to Connect")
    cursor = connection.cursor(dictionary=True)
    #return connection, cursor
    yield cursor
    if commit:
        connection.commit()
    cursor.close()
    connection.close()

def fetch_all_records():
    #connection, cursor = get_db_cursor()
    with get_db_cursor() as cursor:
        cursor.execute("SELECT * FROM expenses")
        expenses = cursor.fetchall()
        for expense in expenses:
            print(expense)


def fetch_expenses_for_date(expense_date):
    logger.info(f"fetch_expenses_for_date called with {expense_date}")
    #connection, cursor = get_db_cursor()
    with get_db_cursor() as cursor:
        cursor.execute("SELECT * FROM expenses where expense_date = %s", (expense_date,))
        expenses = cursor.fetchall()
        for expense in expenses:
            print(expense)
        return expenses


def insert_expense(expense_date, amount, category, notes):
    logger.info(f"insert_expense called with date: {expense_date}, amount:  {amount}, category: {category}, notes: {notes}")
    with get_db_cursor(commit=True) as cursor:
        cursor.execute("INSERT INTO expenses (expense_date, amount, category, notes) values (%s, %s, %s, %s)",(expense_date, amount, category, notes))

def delete_expense_for_date(expense_date):
    logger.info(f"delete_expense_for_date called with {expense_date}")
    with get_db_cursor(commit=True) as cursor:
        cursor.execute("Delete FROM expenses where expense_date = %s", (expense_date,))

def fetch_expense_summary(start_date, end_date):
    logger.info(f"fetch_expense_summary called with start: {start_date}, end: {end_date}")
    with get_db_cursor() as cursor:
        cursor.execute("SELECT category, SUM(amount) as total from expenses where expense_date between %s AND %s group by category;",(start_date, end_date))
        data = cursor.fetchall()
        return data


if __name__ == "__main__":
    expenses = fetch_expenses_for_date("2024-09-30")
    print(expenses)
    insert_expense("2024-08-25", 40, "Food", "Eat tasty Pizza")
    #delete_expense_for_date("2024-08-25")
    summary = fetch_expense_summary("2024-08-01", "2024-08-05")
    for record in summary:
        print(record)