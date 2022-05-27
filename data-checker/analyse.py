import csv
import json
import os


def get_checkpoint():
    f = open('checkpoint.json',)
    return json.load(f)["checkpoint"]


def write_checkpoint(val=0):
    checkpoint = val if val != 0 else get_checkpoint() + 10
    with open("checkpoint.json", "w") as outfile:
        json.dump({"checkpoint": checkpoint - 1}, outfile)


def analyse_data():
    checkpoint = get_checkpoint()

    with open('master.csv', newline='') as f:
        reader = csv.reader(f)
        new_csv = []
        for i, row in enumerate(reader):
            new_csv.append(row)
            if i == checkpoint:
                print(row)
                break
        with open('new.csv', 'w', encoding='UTF8') as f2:
            writer = csv.writer(f2)
            for row in new_csv:
                writer.writerow(row)

    print(os.system("bittytax new.csv"))
    os.system("rm new.csv")
    # os.system("rm BittyTax_Report.pdf")


def tally_asset(asset, wallet):
    checkpoint = get_checkpoint()

    with open('master.csv', newline='') as f:
        reader = csv.reader(f)
        total = 0
        date = 0
        for i, row in enumerate(reader):
            if i <= checkpoint:
                if row[10] == wallet:
                    # buy
                    if row[2] == asset:
                        total += float(row[1])
                    # sell
                    if row[5] == asset:
                        total -= float(row[4])
                    # fee
                    if row[8] == asset:
                        total -= float(row[7])
            else:
                date = row[11]
                break

    print("Total:\t{}\t{}".format(total, date))


def analyse_data_loop():
    analyse_data()

    print("------------\n")
    cont_progress = input("Continue? y / n\n")

    return cont_progress == "y"


def prompt():
    print("---------------------------------------\n")
    print("Welcome, sir! Please choose your option:\n")
    print("\t>\tCheck from line (p)")
    print("\t>\tAuto-check loop (l)")
    print("\t>\tCheck balance at line (o)")
    return input()


##
option = prompt()
if (option == "p"):
    line = input("Enter line number:\n")
    write_checkpoint(int(line))
    analyse_data()

if (option == "l"):
    while(analyse_data_loop()):
        write_checkpoint(line)

if (option == "o"):
    line = input("Enter line number:\n")
    asset = input("Asset:\n")
    wallet = input("Wallet:\n")
    write_checkpoint(int(line))
    tally_asset(asset, wallet)
