import sqlite3
import os

class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = None

    def connect(self):
        """Подключение к базе данных."""
        self.connection = sqlite3.connect(self.db_name)

    def close(self):
        """Закрытие подключения к базе данных."""
        if self.connection:
            self.connection.close()

    def execute_query(self, query, params=None):
        """Выполнение произвольного SQL-запроса."""
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        self.connection.commit()
        return cursor.fetchall()

    def setup_database(self):
        """Создание таблиц, если они не существуют."""
        self.execute_query('''
        CREATE TABLE IF NOT EXISTS Students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            surname TEXT NOT NULL,
            age INTEGER NOT NULL,
            city TEXT NOT NULL
        );
        ''')
        self.execute_query('''
        CREATE TABLE IF NOT EXISTS Courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            time_start TEXT NOT NULL,
            time_end TEXT NOT NULL
        );
        ''')
        self.execute_query('''
        CREATE TABLE IF NOT EXISTS Student_courses (
            student_id INTEGER NOT NULL,
            course_id INTEGER NOT NULL,
            FOREIGN KEY (student_id) REFERENCES Students (id),
            FOREIGN KEY (course_id) REFERENCES Courses (id)
        );
        ''')

    def is_data_populated(self):
        """Проверка, заполнены ли таблицы данными."""
        students = self.execute_query("SELECT * FROM Students")
        courses = self.execute_query("SELECT * FROM Courses")
        student_courses = self.execute_query("SELECT * FROM Student_courses")
        return bool(students) and bool(courses) and bool(student_courses)

    def populate_tables(self):
        """Заполнение таблиц данными, если они пустые."""
        if not self.is_data_populated():
            self.execute_query("INSERT INTO Students (id, name, surname, age, city) VALUES (?, ?, ?, ?, ?)", (1, 'Max', 'Brooks', 24, 'Spb'))
            self.execute_query("INSERT INTO Students (id, name, surname, age, city) VALUES (?, ?, ?, ?, ?)", (2, 'John', 'Stones', 15, 'Spb'))
            self.execute_query("INSERT INTO Students (id, name, surname, age, city) VALUES (?, ?, ?, ?, ?)", (3, 'Andy', 'Wings', 45, 'Manhester'))
            self.execute_query("INSERT INTO Students (id, name, surname, age, city) VALUES (?, ?, ?, ?, ?)", (4, 'Kate', 'Brooks', 34, 'Spb'))
            self.execute_query("INSERT INTO Courses (id, name, time_start, time_end) VALUES (?, ?, ?, ?)", (1, 'python', '21.07.21', '21.08.21'))
            self.execute_query("INSERT INTO Courses (id, name, time_start, time_end) VALUES (?, ?, ?, ?)", (2, 'java', '13.07.21', '16.08.21'))
            self.execute_query("INSERT INTO Student_courses (student_id, course_id) VALUES (?, ?)", (1, 1))
            self.execute_query("INSERT INTO Student_courses (student_id, course_id) VALUES (?, ?)", (2, 1))
            self.execute_query("INSERT INTO Student_courses (student_id, course_id) VALUES (?, ?)", (3, 1))
            self.execute_query("INSERT INTO Student_courses (student_id, course_id) VALUES (?, ?)", (4, 2))

    def get_students_older_than_30(self):
        """Получение студентов старше 30 лет."""
        return self.execute_query("SELECT * FROM Students WHERE age > 30")

    def get_students_in_python_course(self):
        """Получение студентов, проходящих курс по Python."""
        return self.execute_query('''
            SELECT Students.*
            FROM Students
            JOIN Student_courses ON Students.id = Student_courses.student_id
            JOIN Courses ON Courses.id = Student_courses.course_id
            WHERE Courses.name = 'python'
        ''')

    def get_students_in_python_course_from_spb(self):
        """Получение студентов из Spb, проходящих курс по Python."""
        return self.execute_query('''
            SELECT Students.*
            FROM Students
            JOIN Student_courses ON Students.id = Student_courses.student_id
            JOIN Courses ON Courses.id = Student_courses.course_id
            WHERE Courses.name = 'python' AND Students.city = 'Spb'
        ''')

# Тестирование функционала
if __name__ == "__main__":
    db_file = "students_courses.db"
    db = DatabaseManager(db_file)

    if not os.path.exists(db_file):
        print("Создание новой базы данных...")
        db.setup_database()
        db.populate_tables()
    else:
        print("База данных уже существует. Пропуск создания и заполнения.")

    print("Студенты старше 30 лет:")
    print(db.get_students_older_than_30())

    print("Студенты на курсе Python:")
    print(db.get_students_in_python_course())

    print("Студенты на курсе Python из Spb:")
    print(db.get_students_in_python_course_from_spb())

    db.close()
