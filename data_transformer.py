import pandas as pd
import json
import time
import copy


# Función para extraer los datos de la respuesta.
# La entrada es un json.

def extract(input_json):
    data = input_json["results"][0]["result"]["data"]
    dm0 = data["dsr"]["DS"][0]["PH"][0]["DM0"]
    if len(dm0) > 0:
        columns_types = dm0[0]["S"]
        columns = map(lambda item: item["GroupKeys"][0]["Source"]["Property"]
                      if item["Kind"] == 1 else item["Value"], data["descriptor"]["Select"])
        value_dicts = data["dsr"]["DS"][0].get("ValueDicts", {})

        reconstruct_arrays(columns_types, dm0)
        expand_values(columns_types, dm0, value_dicts)

        replace_newlines_with(dm0, "")

        for row in dm0:
            if row["C"][0] is None:
                row["C"][0] = "N/A"

        return list(columns), dm0
    else:
        return [], []


def reconstruct_arrays(columns_types, dm0):
    # fixes array index by applying
    # "R" bitset to copy previous values
    # "Ø" bitset to set null values
    lenght = len(columns_types)
    for item in dm0:
        currentItem = item["C"]
        if "R" in item or "Ø" in item:
            copyBitset = item.get("R", 0)
            deleteBitSet = item.get("Ø", 0)
            for i in range(lenght):
                if is_bit_set_for_index(i, copyBitset):
                    currentItem.insert(i, prevItem[i])
                elif is_bit_set_for_index(i, deleteBitSet):
                    currentItem.insert(i, None)
        prevItem = currentItem


def is_bit_set_for_index(index, bitset):
    return (bitset >> index) & 1 == 1

# substitute indexes with actual values


def expand_values(columns_types, dm0, value_dicts):
    for (idx, col) in enumerate(columns_types):
        if "DN" in col:
            for item in dm0:
                dataItem = item["C"]
                if isinstance(dataItem[idx], int):
                    valDict = value_dicts[col["DN"]]
                    dataItem[idx] = valDict[dataItem[idx]]


def replace_newlines_with(dm0, replacement):
    for item in dm0:
        elem = item["C"]
        for i in range(len(elem)):
            if isinstance(elem[i], str):
                elem[i] = elem[i].replace("\n", replacement)


def convert_to_dataframe(columns, dm0_data, dims):
    if not columns and not dm0_data:
        # Verifica si 'dims' es un entero; si no, usa su longitud
        num_dims = len(dims)

        # Si columns y dm0_data están vacíos, crea un DataFrame con el mismo número de columnas que 'dims'
        columns = ["Dim" + str(i + 1) for i in range(num_dims)]
        columns.append("M0")  # Añade la columna "M0"
        df = pd.DataFrame(columns=columns)
        # Rellena con "N/A" para todas las columnas excepto "M0"
        for col in columns[:-1]:
            df[col] = ["N/A"]
        # Rellena con 0 para la columna "M0"
        df["M0"] = [0]
        return df

    df = pd.DataFrame(columns=columns)
    for i, row in enumerate(dm0_data):
        df.loc[i] = row["C"]

    # Rellena las celdas vacías con "N/A"
    df.fillna("N/A", inplace=True)

    # Elimina 'L' de la columna 'M0' y convierte a entero
    df['M0'] = df['M0'].str.replace('L', '').astype(int)

    return df


# Función para adaptar una query_template en función de las dimensiones que se
# quieren consultar a PBI para luego guardar

def adjust_query(json_payload, dimensions, year=None, month=None):

    # Convertir la cadena JSON en un objeto dict de Python
    if isinstance(json_payload, str):
        payload_bk = json.loads(json_payload)
    else:
        payload_bk = json_payload

    payload = copy.deepcopy(payload_bk)

    # Obtener una referencia a la sección 'From' de la consulta
    from_section = payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['From']

    # Eliminar "From" > "Entity":"date" si no se especifican año y mes
    if year is None and month is None:
        from_section = [
            entry for entry in from_section if entry.get('Entity') != 'date']

    # Filtrar la sección 'From' basada en la lista de dimensiones
    filtered_from_section = [entry for entry in from_section if entry['Name']
                             in dimensions or entry['Name'] == 'm' or entry['Name'] == 'ym']
    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['From'] = filtered_from_section

    # Obtener una referencia a la sección 'Select' de la consulta
    select_section = payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['Select']

    # Filtrar la sección 'Select' basada en la lista de dimensiones
    filtered_select_section = [
        entry for entry in select_section
        if (
            ('Column' in entry and entry['Column']['Expression']['SourceRef']['Source'] in dimensions) or
            ('Measure' in entry)
        )
    ]

    # Cambiar "Measure" > "Property" y "Name" si no se especifican año y mes
    if year is None and month is None:
        for entry in filtered_select_section:
            if 'Measure' in entry:
                entry['Measure']['Property'] = "Peregrinos Ultimo Dia Prueba 2"
                entry['Name'] = "Medidas.Peregrinos Ultimo Dia Prueba 2"

    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['Select'] = filtered_select_section

    # Ajustar la sección 'Projections' con una lista numerada
    projections_length = len(dimensions) + 1  # +1 como se especificó
    projections_list = list(range(projections_length))
    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand'][
        'Binding']['Primary']['Groupings'][0]['Projections'] = projections_list

    # Obtener una referencia a la sección 'Where' de la consulta para acceder a las condiciones
    where_section = payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query'].get('Where', [
    ])

    # Eliminar las condiciones "in" y modificar "not" si no se especifican año y mes
    if year is None and month is None:
        where_section = [
            condition for condition in where_section if 'In' not in condition.get('Condition', {})]
        for condition in where_section:
            if 'Not' in condition.get('Condition', {}):
                condition['Condition']['Not']['Expression']['In']['Expressions'][0]['Measure']['Property'] = "Peregrinos Ultimo Dia Prueba 2"
    else:
        # Recorrer cada condición para buscar los campos de "Año" y "Mes"
        for condition in where_section:
            if 'Condition' in condition and 'In' in condition['Condition']:
                column_property = condition['Condition']['In']['Expressions'][0]['Column'].get(
                    'Property', "")

                # Si encontramos el campo de "Año", ajustamos el valor del Literal
                if column_property == "Año" and year is not None:
                    condition['Condition']['In']['Values'][0][0]['Literal']['Value'] = str(
                        year) + 'L'

                # Si encontramos el campo de "Mes", ajustamos el valor del Literal
                if column_property == "Mes" and month is not None:
                    condition['Condition']['In']['Values'][0][0]['Literal']['Value'] = f"'{month}'"

    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['Where'] = where_section

    return payload


def adjust_query_per_day(json_payload, dimensions, date):
    # Función para adaptar una query_template en función de las dimensiones que se
    # quieren consultar a PBI para luego guardar

    # Convertir la cadena JSON en un objeto dict de Python
    if isinstance(json_payload, str):
        payload_bk = json.loads(json_payload)
    else:
        payload_bk = json_payload

    payload = copy.deepcopy(payload_bk)

    # Obtener una referencia a la sección 'From' de la consulta
    from_section = payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['From']

    # Eliminar "From" > "Entity":"date" si no se especifican año y mes
    # from_section = [entry for entry in from_section if entry.get('Entity') != 'date']

    # Filtrar la sección 'From' basada en la lista de dimensiones
    filtered_from_section = [entry for entry in from_section if entry['Name']
                             in dimensions or entry['Name'] == 'm' or entry['Name'] == 'ym']
    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['From'] = filtered_from_section

    # Obtener una referencia a la sección 'Select' de la consulta
    select_section = payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['Select']

    # Filtrar la sección 'Select' basada en la lista de dimensiones
    filtered_select_section = [
        entry for entry in select_section
        if (
            ('Column' in entry and entry['Column']['Expression']['SourceRef']['Source'] in dimensions) or
            ('Measure' in entry)
        )
    ]

    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['Select'] = filtered_select_section

    # Ajustar la sección 'Projections' con una lista numerada
    projections_length = len(dimensions) + 1  # +1 como se especificó
    projections_list = list(range(projections_length))
    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand'][
        'Binding']['Primary']['Groupings'][0]['Projections'] = projections_list

    # Obtener una referencia a la sección 'Where' de la consulta para acceder a las condiciones
    where_section = payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query'].get('Where', [
    ])

    # Recorrer cada condición para buscar los campos de "Año" y "Mes"
    for condition in where_section:
        if 'Condition' in condition and 'In' in condition['Condition']:
            column_property = condition['Condition']['In']['Expressions'][0]['Column'].get(
                'Property', "")

            # Si encontramos el campo de "Date", ajustamos el valor del Literal
            if column_property == "Date":
                condition['Condition']['In']['Values'][0][0]['Literal']['Value'] = f"'{date}'"

    payload['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Query']['Where'] = where_section

    return payload


def extract_date_last_day(json_data):
    # Initialize an empty dictionary to store the extracted data
    data_dict = {}

    # Navigate through nested dictionaries and lists to find the data for "M1" and "M0"
    data = json_data.get('results', [{}])[0].get('result', {}).get(
        'data', {}).get('dsr', {}).get('DS', [{}])[0]

    # Extract and reformat the date from the "M1" key using time.strptime and time.strftime
    m1_value = data.get('M1', 'N/A')
    try:
        # Extract the date string from the original string
        date_string = m1_value.split(": ")[1].strip("'")

        # Parse the date string into a time struct
        time_struct = time.strptime(date_string, "%d/%m/%Y")

        # Reformat the time struct into the desired "YYYY-MM-DD" format
        formatted_date = time.strftime("%Y-%m-%d", time_struct)
        data_dict['date'] = formatted_date
    except (ValueError, IndexError):
        data_dict['date'] = 'N/A'

    # Extract the number of pilgrims from the key "M0" ending with 'L'
    ph_data = data.get('PH', [{}])[0].get('DM0', [{}])[0].get('M0', 'N/A')
    if str(ph_data).endswith('L'):
        data_dict['pilgrims'] = ph_data[:-1]  # Remove the 'L' at the end
    else:
        data_dict['pilgrims'] = 'N/A'

    return data_dict
