import cleaning
import db









if __name__ == '__main__':
    print("Hello World\nHi")
    df = cleaning.get_cleaned_dataset()
    db.save_to_db(df, False)
