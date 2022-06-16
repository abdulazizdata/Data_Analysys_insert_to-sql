import pandas as pd
import sqlite3

# Step - 4 Sqlilte
# # Connecting to sqlite
conn = sqlite3.connect('sqlite3.db')
# # Creating a cursor object using the cursor() method
cursor = conn.cursor()
# cursor.execute('''CREATE TABLE "customers" (
#
#      "gender"   TEXT,
#
#      "firstname"    TEXT,
#
#      "Lastname" TEXT,
#
#      "email"    TEXT,
#
#      "age"  INTEGER,
#
#      "city" TEXT,
#
#      "country"  TEXT
#
#
#
# );''')
# Step - 1, df1
df2_column_name = ["Age", "City", "Gender", "Name", "Email"]
df1 = pd.read_csv("only_wood_customer_us_1.csv")
df2 = pd.read_csv("only_wood_customer_us_2.csv", sep=';', header=None)
df2.columns = df2_column_name
df3 = pd.read_csv("only_wood_customer_us_3.csv", sep="[\t,]", engine='python')


def gender(x):  # I used this func to correct the data in Gender columin
    x = x.split("_")[-1]
    if x == '1' or x == 'F':
        x = "Female"
    elif x == '0' or x == 'M':
        x = "Male"
    return x


def name(x):  # I used this func to correct the data in Name columin
    try:
        return x[1:-3].title() if ord(x[0]) == 92 else x.title()
    except:
        pass


df1.Gender = df1.Gender.apply(gender)  # The Gender column has been corrected.
df1.FirstName = df1.FirstName.apply(name)  # The FirstName column has been corrected.
df1.LastName = df1.LastName.apply(name)  # The LastName column has been corrected.
df1.Email = df1.Email.map(lambda x: x.lower())  # The Email column has been corrected.
df1.City = df1.City.map(lambda x: x.replace('-', ' ').replace('_', ' ').title())  # The City column has been corrected.
df1.Country = df1.Country.map(lambda x: 'USA' if x != "USA" else x)  # The Country column has been corrected.
df1.dropna(inplace=True)  # There is only 1 row with NaN value, so this has been removed.
del df1["UserName"]
# Step - 2, df2
lis = df2["Name"].to_list()


def age(x):  # I used this func to correct the data in Age columin
    re = ""
    for i in x:
        if i.isdigit():
            re += i
    return int(re)


def email(x):  # I used this func to correct the data in Email columin
    re = ""
    try:
        x = x.split("_")[1:]
        if len(x) > 1:
            re += x[0] + "_" + x[1]
        else:
            re += x[0]
    except:
        pass
    return re.lower()


def name2(fullname):  # I used this func for the column that came with First Name and Last Name
    F_name, L_name = [], []
    for x in fullname:
        x = x.split("_")[-1]
        x = x.replace('"', " ").replace(chr(92), " ").split()
        F_name.append(x[0].title())
        L_name.append(x[1].title())
    return F_name, L_name


w = name2(lis)
df2["FirstName"] = w[0]
df2["LastName"] = w[1]
del df2["Name"]
df2["Gender"] = df2['Gender'].apply(gender)  # The Gender column has been corrected.
df2.City = df2.City.map(lambda x: x.replace('-', ' ').replace('_', ' ').title())  # The City column has been corrected.
df2.Age = df2.Age.apply(age)  # The Age column has been corrected
df2["Country"] = df2.City.map(lambda x: "USA" if x != None else "")  # The Country column has been corrected.
df2.Email = df2.Email.apply(email)  # The Email column has been corrected.


# Step - 3, df3
def city(x):  # I used this func to correct the data in City columin
    re = ''
    x = x.split("_")[1:]
    for i in x:
        re += " " + i
    return re.strip().replace("-", " ").title()


names = df3.Name.to_list()
both = name2(names)
df3["FirstName"] = both[0]
df3["LastName"] = both[1]
del df3["Name"]
df3["Country"] = df3.City.map(lambda x: "USA" if x != None else "")  # The Country column has been corrected.
df3.Gender = df3.Gender.apply(gender)  # The Gender column has been corrected.
df3.City = df3.City.apply(city)  # The City column has been corrected.
df3.Email = df3.Email.apply(email)  # The Email column has been corrected# .
df3.Age = df3.Age.apply(age)  # The Age column has been corrected.
all_df = pd.concat([df1, df2, df3])
print(all_df.shape)
# Insert Data to table(Step4)
i = 0
for i in range(len(all_df)):
    genders = all_df.iloc[i, 0]
    first_names = all_df.iloc[i, 1]
    last_names = all_df.iloc[i, 2]
    emails = all_df.iloc[i, 3]
    ages = all_df.iloc[i, 4]
    cities = all_df.iloc[i, 5]
    countries = all_df.iloc[i, 6]
    i += 1
    cursor.execute('''INSERT INTO customers(
           gender, firstname, Lastname,email, age, city, country) VALUES
           (?,?,?,?,?,?,?)''', [genders, first_names, last_names, emails, int(ages), cities, countries])
conn.commit()
print("Records inserted........")
# Closing the connection
conn.close()
