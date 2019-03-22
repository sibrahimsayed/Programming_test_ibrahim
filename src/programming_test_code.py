import pandas
import os
import math

def read_csv_file_data(file_path):
    """
    This function read given csv file path
    :param file_path: file path of data file should be in csv format(String)
    :return: dataframe (dataframe)
    """
    if os.path.exists(file_path):
        df = pandas.read_csv(file_path)
    else:
        raise ValueError('ERROR: file_path doesnt exist in read_csv_file_data()')
    return df

#Question 1 part 1
'---------------------------------------------------------------------------------------------------------------------'
def evaluate_ratio_values_for_all_flights(df_flight_info):
    """
    For every class in every flight, this function obtains the ratio of the value of the class to the value of the
    next ranked class .  This function adds a the ratio column to the data frame.
    :param df_flight_info: Pandas Data Frame that stores the fare class information for flights
    :return: df_flight_info: the same Data Frame where the last column is the Ration of value of a flight to its next
    class rank
    """
    df_flight_info['RatioToNextClass'] = df_flight_info['FareValue'].astype(float)
    df_flight_info.sort_values(by=['FlightId', 'FareClassRank'])
    row = 0
    while row < len(df_flight_info):
        while df_flight_info.iloc[row]['FlightId'] == df_flight_info.iloc[row + 1]['FlightId']:
            current_class_value = df_flight_info.iloc[row]['FareValue']
            next_class_value = df_flight_info.iloc[row + 1]['FareValue']
            ratio = float(current_class_value / next_class_value)
            df_flight_info.at[row, 'RatioToNextClass'] = ratio
            row = row + 1
            if row + 1 == len(df_flight_info):
                break
        df_flight_info.at[row, 'RatioToNextClass'] = 1
        row = row + 1
    return df_flight_info

def run_code_for_question_one_part_one():
    """
    This function runs the code for Question one part one
    """
    df_flights_info = read_csv_file_data("../Data/T_Fare_Flight_Q1.csv")
    df_flights_info = evaluate_ratio_values_for_all_flights(df_flights_info)
    df_flights_info.to_csv("../Output/Question_1_Part_1_ClassesRatio.csv")
    print(df_flights_info.head())

#Question 1 part 2
'---------------------------------------------------------------------------------------------------------------------'

def evaluate_percentage_of_class_for_each_flight(df_flights_info):
    """
    This function adds a column the contains the percentage of booking for each class at each flight.
    :param df_flights_info: (pandas DataFrame) contains information of fights
    :return: df_flights_info: (pandas DataFrame) contains information of fights including the additional column
    """
    df_new = df_flights_info[['FlightId', 'FareClass', 'Booking']]
    df_new = df_new.pivot(index='FlightId', columns='FareClass', values='Booking').astype(float)
    df_new.loc[:, 'Total'] = df_new.sum(axis=1).astype(float)
    for row, col in df_new.iterrows():
        for item in list(df_new):
            number_booking = df_new.loc[row, item]
            total_booking = df_new.loc[row]['Total']
            percentage = float(number_booking / total_booking)
            df_new.at[row, item] = percentage
    df_new = df_new.drop(columns=['Total'])
    df_new = df_new.stack()
    df_new = df_new.reset_index(level=[0, 1])
    df_flights_info = pandas.merge(df_flights_info, df_new, how='left', on=['FlightId', 'FareClass'])
    df_flights_info.rename(columns={0: 'Percentage'}, inplace=True)
    return df_flights_info

def run_code_for_question_one_part_two():
    """
    This function runs thhe code for question one part two
    """
    df_flights_info = read_csv_file_data("../Data/T_Fare_Flight_Q1.csv")
    df_flights_info = evaluate_percentage_of_class_for_each_flight(df_flights_info)
    df_flights_info.to_csv("../Output/Question_1_Part_2_percentages.csv")
    print(df_flights_info.head())

#Question 2
'---------------------------------------------------------------------------------------------------------------------'

def estimate_number_of_booking(df_T_fare_info, df_T_flight_info, flight, df_fare_booking_number):
    """
    For a given flight, the function sets the number of booking for each class of that flight to the floor(x) and gets
     the decimal part of x, where x = (total number of booking)*(percentage Booking).
    :param df_T_Fare_info: the type is Pandas DataFrame: contains the parentage of classes for each flight
    :param df_T_Flight_info: the type is Pandas DataFrame: contains the total number of booking for each flight
    :param Flight: the type is str: Flight Id
    :param df_Fare_Booking_Number: the type is Pandas DataFrame: contains the number of booking for each class
    :return: df_Fare_Booking_Number: (Pandas DataFrame) contains the number of booking for each class
    """
    total_booking = df_T_flight_info.loc[flight, 'total_booking']
    for class_type in list(df_T_fare_info):
        percentage_booking = df_T_fare_info.loc[flight][class_type]
        number_of_booking = percentage_booking * total_booking
        integer_number = int(math.floor(number_of_booking))
        df_fare_booking_number.at[flight, class_type] = integer_number
    return df_fare_booking_number

def get_decimal_part(df_T_fare_info, df_T_flight_info, flight, df_number_booking):
    """
    For a given flight, the function the decimal part of (x), where x = (total number of booking)*(percentage Booking).
    :param df_T_Fare_info: the type is Pandas DataFrame: contains the parentage of classes for each flight
    :param df_T_Flight_info: the type is Pandas DataFrame: contains the total number of booking for each flight
    :param Flight: the type is str: Flight Id
    :param df_Fare_Booking_Number: the type is Pandas DataFrame: contains the number of booking for each class
    :return: dic_decimal_part: (dictionary) contains decimal part associated with each class
    """
    dic_decimal_part = {}
    total_booking = df_T_flight_info.loc[flight, 'total_booking']
    for class_type in list(df_T_fare_info):
        percentage_booking = df_T_fare_info.loc[flight][class_type]
        number_of_booking = percentage_booking * total_booking
        integer_number = int(math.floor(number_of_booking))
        dic_decimal_part[class_type] = number_of_booking - integer_number
    return dic_decimal_part

def improves_estimation(df_T_fare_booking_number_info, flight='', dic_fare_frac={}):
    """
    For a given flight, this function improves the estimation for number of bookings. Is is doing that by using the
    decimal part of number of booking. Let x be the maximum integer less than the sum of the fractional parts.
    Then this function increase the booking number by one for x classes. The function increases the number
    of booking for classes with highest decimal part values.
    :param df_T_fare_booking_number_info: Pandas DataFrame stores the information of flights, which is the number of
    booking of each class
    :param flight: the Flight ID type and the type is str
    :param dic_fare_frac: is a dictionary stores fractional value between zero and one. It corresponds to the decimal
    part of the estimated number of booking.
    :return: df_T_fare_booking_number_info: a pandas DataFrame contains the stores the information of flights, which is
    the number of booking of each class
    """
    counter = - 1
    List_sorted_fares = sorted(dic_fare_frac, key=dic_fare_frac.get)
    Comulative_fraction = sum(dic_fare_frac.values())
    increments = int(round(Comulative_fraction))
    while increments > 0:
        fare = List_sorted_fares[counter]
        df_T_fare_booking_number_info.at[flight, fare] = df_T_fare_booking_number_info.loc[flight, fare] + 1
        counter = counter - 1
        increments = increments - 1
    return df_T_fare_booking_number_info

def obtain_number_of_booking(df_T_fare_info, df_T_flight_info):
    """
    This function is to estimate and obtain the number of booking of each fare class for each flights. The estimated
    numbers are integer.
    :param df_T_fare_info: is a pandas data frame. it contains the percentage of each fare class in each flight
    :param df_T_Flight_info: is a pandas data frame contains the capacity and total booking information for each flight.
    :return: a pandas data frame contains the number of booking for each fare class of flights
    """
    df_number_booking = df_T_fare_info.astype(int)
    for row, col in df_T_fare_info.iterrows():
        df_number_booking = estimate_number_of_booking(df_T_fare_info, df_T_flight_info, row, df_number_booking)
        dic_class_frac = get_decimal_part(df_T_fare_info, df_T_flight_info, row, df_number_booking)
        df_number_booking = improves_estimation(df_number_booking, flight=row, dic_fare_frac=dic_class_frac)
    return df_number_booking

def read_data_for_question_2():
    """
    This function reads the data files for question 2.
    :return:  two dataframes (df_T_fare_info),(df_T_Flight_info): (df_T_fare_info) is pandas dataframe contains the
    table that defines the class fare percentage of booking for each flight: (df_T_Flight_info) is pandas dataframe
    contains the table that defines for each flight the capacity and the total flight
    """
    df_T_fare_info = read_csv_file_data("../Data/T_Fare_info_Q2.csv")
    df_T_fare_info.set_index('FlightId', inplace=True)
    df_T_flight_info = read_csv_file_data("../Data/T_Flight_info_Q2.csv")
    df_T_flight_info.set_index('FlightId', inplace=True)
    df_T_fare_info.dropna(inplace=True)
    df_T_flight_info.dropna(inplace=True)
    return df_T_fare_info, df_T_flight_info

def run_code_for_question_two():
    """
    This function to run the code for Question 2:
    """
    df_T_fare_info, df_T_flight_info = read_data_for_question_2()
    df_T_fare_booking_info = obtain_number_of_booking(df_T_fare_info, df_T_flight_info)
    df_T_fare_booking_info['SumOfNumberOfBooking'] = df_T_fare_booking_info[list(df_T_fare_booking_info.columns)].sum(
        axis=1)
    df_T_fare_booking_info.to_csv("../Output/Question_2.csv")
    print(df_T_fare_booking_info.head)

if __name__ == '__main__':
    print('Hello: Thanks For running the code')
    while (True):
        print('Please choose one of the following options:')
        print('\tenter (1) if you want to run the code for question 1 part 1')
        print('\tenter (2) if you want to run the code for question 1 part 2')
        print('\tenter (3) if you want to run the code for question 2')
        print('\tenter (4) to exit')
        select = int(input(">>:"))
        if select == 1:
            print('You chose option 1')
            print('i am running the code for question one part one')
            print('here is a subset of the result')
            run_code_for_question_one_part_two()
            print(
                '\n check the results in "Programming_test_ibrahim\Output\Question_1_Part_1_ClassesRatio.csv"')
        elif select == 2:
            print('\tthanks for choosing option 2')
            print('\tYou chose option 2')
            print('\ti am running the code for question one part two')
            print('here is a subset of the result')
            run_code_for_question_one_part_two()
            print(
                '\n check the results in "Programming_test_ibrahim\Output\Question_1_Part_2_percentages.csv"')
        elif select == 3:
            print('\tthanks for choosing option 3')
            print('\tYou chose option 3')
            print('\ti am running the code for question two')
            print('here is a subset of the result')
            run_code_for_question_two()
            print(
                '\n check the results in "Programming_test_ibrahim\Output\Question_2.csv"')
        elif select == 4:
            break
        else:
            print("please enter (1), (2), or (3)")
            continue
        print('\n\tenter: "n" to exit or "y" to repeat')
        select = input('').lower()
        if select == 'n':
            break
