import requests
import pandas as pd
import jmespath
from decouple import config

EXCHANGERATE_API_KEY = config("EXCHANGERATE_API_KEY")
CURRENCIES = requests.get(f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/USD").json()
# Para mas información sobre esta API, visitar: https://www.exchangerate-api.com/
mapper_currency = CURRENCIES['conversion_rates']


def get_category_country_list(countries: list, category: str = "1055", limit: int = 1):
    """
    Toma una lista de site_id (paises) y realiza un GET a la API de MELI sobre cierta categoría específica
    (1055 corresponde a Smartphones) para cada país (site_id) en la lista. Limit me permite mover el offset para poder
    obtener hasta 1000 request por país.
    :param countries: list [str] - Lista de paises representados por su site_id
    :param category: str - categoría específica
    :param limit: int - numero de offsets que seran consultados
    :return: list [dict]
    """
    if isinstance(countries, str):  # Catch single value passed as string
        countries = [countries]
    all_results = []
    for country_id in countries:
        for i in range(limit):
            offset = 50 * i
            result = requests.get(
                f'https://api.mercadolibre.com/sites/{country_id}/search?category={country_id + category}&offset={offset}').json()
            if len(result.get('results')) == 0:  # No more results
                break
            if result.get('error', None) is not None:
                print(f'{country_id + category} on the offset {offset} returned: {result.get("error")}')
                break
            all_results.append(result["results"])
    return [item for sublist in all_results for item in sublist]  # flatten the list


def single_df(result: dict, features: list = None, attributes: list = None):
    """
    Crea un DataFrame base con los datos de una (1) request.
    :param result: dict - Request proveniente de un llamado GET a la API. Formato JSON
    :param features: list [str | tuple] - Lista de features (str) que vamos a buscar en nuestra request.
    En caso de que queramos buscar una feature que requiera una query anidada debemos pasar una tuple.
    :param attributes: list [str] - Lista de atributos específicos a ser explorados en request['attributes']
    """
    if not features:  # default features que considero utiles para el modelo
        features = ["site_id", "price", "currency_id", "available_quantity", "sold_quantity", "buying_mode",
                    "listing_type_id", "condition", "accepts_mercadopago", "original_price",
                    ('seller', "seller_reputation", "level_id"), ('shipping', "free_shipping"),
                    ('address', 'state_name')
                    ]
    if not attributes:
        attributes = ['BRAND', 'MODEL']
    df = pd.DataFrame(index=[0])  # Inicio un df vacío. Como son 1000 casos no deberìa demorar mucho
    for feature in features:
        if isinstance(feature, tuple):
            df['_'.join(feature)] = jmespath.search('.'.join(feature), result)
        else:
            df[feature] = result[feature]
    for attrib in attributes:
        df[attrib] = get_specific_attribute(attrib, result)
    return df


def basic_df(results: list, features: list = None, attributes: list = None):
    """
    Arma un DF con los mismos campos que `single_df` para todas las request obtenidas de un GET a la API de MELI.
    :param results: list [dict] - List de requests. Every request is a dict (json request)
    :param features: list [str | tuple] - Lista de features (str) que vamos a buscar en nuestra request.
    En caso de que queramos buscar una feature que requiera una query anidada debemos pasar una tuple.
    :param attributes: list [str] - Lista de atributos específicos a ser explorados en request['attributes']
    """
    start = True
    for result in results:
        if start:
            df = single_df(result, features, attributes)
            start = False
        else:
            df = pd.concat([df,
                            single_df(result, features, attributes)],
                           ignore_index=True
                           )
    return df


def get_specific_attribute(attribute_id: str, result: dict):
    """
    Hace una query en los atributos de un result y devuelve el value_name del mismo.
    :param attribute_id: id del atributo
    :type attribute_id: dict
    :param result: result obtenida tras realizar un GET a la API de MELI
    :type result: dict
    :return: value_name del atributo
    :rtype: str
    """
    value = jmespath.search(f"attributes[?id=='{attribute_id}'].value_name | [0]", result)
    return value


def calculate_discount_metrics(df):
    """
    Calcula si el producto tiene un descuento, asi como el valor de los descuentos tanto en pesos como USD.
    Para la conversión a dólares se usa el valor al dia de la fecha extraído de https://www.exchangerate-api.com/
    :param df: Dataframe con los valores para cada observación. Output de `basic_df`.
    :type df: pandas.DataFrame
    :return: Dataframe con los campos calculados: ["price_USD","discount","descuento_precio","descuento_USD"]
    :rtype: pandas.DataFrame
    """
    # Precio a USD
    df['price_USD'] = df.apply(lambda row: row.price/mapper_currency[row.currency_id]
                               if row.currency_id != 'USD' else row.price, axis=1)
    # Descuento
    df['discount'] = 1
    df.loc[df.original_price.isna(), 'discount'] = 0  # Si original_price is null entonces no tiene descuento
    # Descuentos en pesos
    df['descuento_precio'] = df.apply(lambda row: row.original_price - row.price if row.discount == 1 else 0, axis=1)
    # Descuentos en USD
    df['descuento_USD'] = df.apply(lambda row: row.descuento_precio/mapper_currency[row.currency_id]
                                   if row.currency_id != 'USD' else row.descuento_precio, axis=1)
    return df
