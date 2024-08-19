import pymysql
import pymysql.cursors

connect_db = pymysql.connect(
    host = "127.0.0.1",
    user = "root",
    password = "12345",
    db = "Task",
    port = 3306,
    charset = "utf8mb4",
    cursorclass = pymysql.cursors.DictCursor
)

#1) 
def Create_Table():
    with connect_db.cursor() as c:
        sql_command = """Create Table Hospitals(
                    Hospital_id int Primary Key Auto_Increment not null,
                    Hospital_Name varchar(100) not null,
                    Bed_Count int not null
                );"""
        c.execute(sql_command)

    

def Create_Table():
    with connect_db.cursor() as c:
        sql_command = """Create Table Doctors(
                    Doctor_id int Primary Key  not null,
                    Doctor_Name varchar(100) not null,
                    Hospital_id int not null,
                    Foreign Key (Hospital_id) references Hospitals(Hospital_id) on delete cascade,
                    Joining_Date date not null,
                    Speciality varchar(30) not null,
                    Salary smallint(6) not null,
                    Experience text
                );"""
        c.execute(sql_command)



def Insert_into():
    with connect_db:
        with connect_db.cursor() as c:
            sql_command = """Insert into Task.Hospitals(Hospital_id,Hospital_Name,Bed_Count)
                        Values
                        (1,"Mayo Clinic",200),
			(2,"Cleveland Clinic,400"),
			(3,"Johns Hopkins",1000),
			(4,"UCLA Medical Center",1500)
            """
            c.execute(sql_command)
        connect_db.commit()




def Insert_into():
    with connect_db:
        with connect_db.cursor() as c:
            sql_command = """Insert into Task.Doctors(Doctor_id,Doctor_Name,Hospital_id,Joining_Date,Speciality,Salary)
                        Values
                        (101,"David",1,"2005-02-10","Pediatric",40000),
                        (102,"Michael",1,"2018-07-23","Oncologist",20000),
                        (103,"Susan",2,"2016-05-19","Garnacologist",25000),
                        (104,"Robert",2,"2017-12-28","Pediatric",28000),
                        (105,"Linda",3,"2004-06-04","Garnacologist",42000),
                        (106,"William",3,"2012-09-11","Dermatalogist",30000),
                        (107,"Richard",4,"2014-08-21","Garnacologist",32000),
                        (108,"Karen",4,"2011-10-17","Radiologist",30000)
            """
            c.execute(sql_command)
        connect_db.commit()


# 2)
def get_data_H_id(Hospital_id):
    with connect_db.cursor() as c:
        sql_command = """Select * from Task.Doctors
                         Where  Hospital_id = %s"""
        c.execute(sql_command,(Hospital_id))
        return c.fetchall()
    




def get_data_D_id(Doctor_id):
    with connect_db.cursor() as c:
        sql_command = """Select * from Task.Doctors
                         Where Doctor_id = %s"""
        c.execute(sql_command,(Doctor_id))
        return  c.fetchall()
    
# 3)
def get_s(Salary):
    with connect_db.cursor() as c:
        sql_command = """Select * from Task.Doctors
                         Where Salary > %s """
        c.execute(sql_command,(Salary))
        return c.fetchall()




def get_s(Speciality):
    with connect_db.cursor() as c:
        sql_command = """Select * from Task.Doctors
                         Where Speciality Like %s"""
        c.execute(sql_command,(Speciality))
        return c.fetchall()

# 4)
def get_d_id(Hospital_id):
    with  connect_db.cursor() as c:
        sql_command = """Select Doctor_Name,Joining_Date,Speciality,Salary,Hospital_Name,Bed_Count from Task.Doctors
                        inner join Hospitals
                        on Task.Doctors.hospital_id =  %s """
        c.execute(sql_command,(Hospital_id))
        return c.fetchall()
    
# 5)
def TIMESTAMPDIFF():
    with connect_db:
        with connect_db.cursor() as c:
            sql_command = """ Update Task.Doctors
                              Set Experience = TimeStampDiff(Year,Joining_Date,"2024-8-20") 
                              Where Doctor_id = 101;                        
                                """
            c.execute(sql_command)
        connect_db.commit()

TIMESTAMPDIFF()
            
            
    