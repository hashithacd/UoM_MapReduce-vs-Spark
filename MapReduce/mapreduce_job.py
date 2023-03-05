#!/usr/bin/env python

import sys
import csv

# Mapper function
def mapper():
    # Read each line from input
    for line in sys.stdin:
        # Parse the CSV row
        row = list(csv.reader([line]))[0]
        
        # Extract the relevant fields
        year = row[0]
        month = row[1]
        day = row[2]
        origin = row[16]
        dest = row[17]
        arr_delay = row[14]
        
        # Emit the key-value pair
        print(f"{year}-{month}-{day}\t{origin},{dest},{arr_delay}")

# Reducer function
def reducer():
    current_date = None
    current_flights = []

    # Read each key-value pair from input
    for line in sys.stdin:
        # Split the line into key and value
        key, value = line.strip().split('\t')

        # If the key is the same as the current date, add the value to the current flights
        if key == current_date:
            current_flights.append(value)
        # Otherwise, process the current flights and start a new date
        else:
            if current_date:
                # Calculate the average arrival delay for each flight
                flight_delays = []
                for flight in current_flights:
                    origin, dest, arr_delay = flight.split(',')
                    if arr_delay != '':
                        flight_delays.append(int(arr_delay))
                avg_delay = sum(flight_delays) / len(flight_delays)

                # Emit the output
                print(f"{current_date}\t{avg_delay:.2f}")

            current_date = key
            current_flights = [value]

    # Don't forget to process the last date
    if current_date == key:
        # Calculate the average arrival delay for each flight
        flight_delays = []
        for flight in current_flights:
            origin, dest, arr_delay = flight.split(',')
            if arr_delay != '':
                flight_delays.append(int(arr_delay))
        avg_delay = sum(flight_delays) / len(flight_delays)

        # Emit the output
        print(f"{current_date}\t{avg_delay:.2f}")

if __name__ == '__main__':
    # Determine whether to run the mapper or the reducer based on the command line argument
    if sys.argv[1] == 'mapper':
        mapper()
    elif sys.argv[1] == 'reducer':
        reducer()
