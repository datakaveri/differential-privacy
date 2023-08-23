import utilities as utils


def suppressConfigurator(dataframe, configDict):
    print('Select the columns that you want to suppress from the output: ')

    # Define a list of columns
    columns = dataframe.columns

    # Display a menu for the user
    print("Choose columns (comma-separated):")

    # Print each column with a corresponding number
    for i, column in enumerate(columns, 1):
        print(f"{i}. {column}")

    # Get user input for multiple choices
    try:
        choices = input("Enter the numbers of the columns you want to suppress (comma-separated): ")
        choices = [int(choice.strip()) for choice in choices.split(",")]
    except ValueError:
        print("Invalid input. Please enter valid numbers separated by commas.")
        exit(1)

    # Check if the choices are within the valid range
    selected_columns = []
    for choice in choices:
        if 1 <= choice <= len(columns):
            selected_columns.append(columns[choice - 1])
        else:
            print(f"Invalid choice: {choice}. Skipping.")

    # Display the selected columns
    if selected_columns:
        print("You selected the following columns for suppression:")
        for column in selected_columns:
            print(column)
    else:
        print("No valid columns selected.")
    configDict['suppressCols'] = selected_columns
    dataframe = utils.suppress(dataframe, configDict)
    suppressedDataframe = dataframe.copy()
    return suppressedDataframe


def pseudonymizeConfigurator(dataframe, configDict):
    print('Select the columns that you want to pseudonymize in the output: ')

    # Define a list of columns
    columns = dataframe.columns

    # Display a menu for the user
    print("Choose columns (comma-separated):")

    # Print each column with a corresponding number
    for i, column in enumerate(columns, 1):
        print(f"{i}. {column}")

    # Get user input for multiple choices
    try:
        choices = input("Enter the numbers of the columns you want to pseudonymize (comma-separated): ")
        choices = [int(choice.strip()) for choice in choices.split(",")]
    except ValueError:
        print("Invalid input. Please enter valid numbers separated by commas.")
        exit(1)

    # Check if the choices are within the valid range
    selected_columns = []
    for choice in choices:
        if 1 <= choice <= len(columns):
            selected_columns.append(columns[choice - 1])
        else:
            print(f"Invalid choice: {choice}. Skipping.")

    # Display the selected columns
    if selected_columns:
        print("You selected the following columns for pseudonymization:")
        for column in selected_columns:
            print(column)
    else:
        print("No valid columns selected.")
    configDict['pseudoCol'] = selected_columns
    dataframe = utils.pseudonymize(dataframe, configDict)
    pseudonymizedDataframe = dataframe.copy()
    return pseudonymizedDataframe