def format_data(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            # Remove round braces and the space after the comma
            # formatted_line = line.replace('(', '').replace(')', '').replace(', ', ',')
            formatted_line = line.replace(',','|')
            outfile.write(formatted_line)

if __name__ == "__main__":
    input_file = '/Users/VISHAL/share/Spark_week_10/assignment/answer1/part-00000'   # Replace with your input file name
    output_file = '/Users/VISHAL/OneDrive/Desktop/output.txt' # Replace with your desired output file name
    format_data(input_file, output_file)

