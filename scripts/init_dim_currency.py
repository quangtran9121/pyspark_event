import pycountry
from sqlalchemy.exc import SQLAlchemyError

from models.common_dim import DimCurrency
from common.logger import get_logger

# Configure logging
logger = get_logger('dim_currency_populate', './logs')


# Function to retrieve currencies using pycountry
def get_all_currencies():
    currencies = []
    for currency in pycountry.currencies:
        # Exclude certain currencies if needed, e.g., historical
        # For simplicity, include all available
        if hasattr(currency, 'alpha_3'):
            currencies.append({
                'currency_name': currency.name,
                'currency_code': currency.alpha_3
            })
    return currencies

# Function to check if a currency exists
def currency_exists(session, currency_code):
    return session.query(DimCurrency).filter_by(currency_code=currency_code).first() is not None

# Function to bulk insert currencies, avoiding duplicates
def bulk_insert_currencies(session, currencies):
    """
    Bulk inserts currencies into the dim_currency table, avoiding duplicates.

    Args:
        session (Session): SQLAlchemy session object.
        currencies (list of dict): List of currency dictionaries.
    """
    inserted_count = 0
    for currency in currencies:
        try:
            if not currency_exists(session, currency['currency_code']):
                new_currency = DimCurrency(
                    currency_name=currency['currency_name'],
                    currency_code=currency['currency_code']
                )
                session.add(new_currency)
                inserted_count += 1
                logger.info(f"Adding currency: {currency['currency_name']} ({currency['currency_code']})")
            else:
                logger.info(f"Currency already exists: {currency['currency_name']} ({currency['currency_code']})")
        except Exception as e:
            logger.error(f"Error processing currency {currency}: {e}")

    try:
        session.commit()
        logger.info(f"Successfully inserted {inserted_count} new currencies.")
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error during commit: {e}")

def gen_dim_currency(session):
    try:
        # Retrieve all currencies
        currencies = get_all_currencies()
        logger.info(f"Retrieved {len(currencies)} currencies from pycountry.")

        # Insert currencies into the database
        bulk_insert_currencies(session, currencies)

        # Optional: Query and display all currencies
        all_currencies = session.query(DimCurrency).order_by(DimCurrency.currency_code).all()
        logger.info("All currencies in the database:")
        for currency in all_currencies:
            logger.info(currency)
    finally:
        logger.info("End func: gen_dim_currency.")