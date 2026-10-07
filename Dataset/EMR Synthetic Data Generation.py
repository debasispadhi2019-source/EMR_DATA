# Databricks notebook source
spark.sql("create catalog if not exists Faker")

spark.sql('use catalog Faker')

spark.sql("CREATE SCHEMA IF NOT EXISTS Faker.emr_data")

spark.sql("CREATE VOLUME IF NOT EXISTS Faker.emr_data.emr_files")

spark.sql("SHOW VOLUMES IN Faker.emr_data").display()

# COMMAND ----------

pip install faker

# COMMAND ----------

import random
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import*
from pyspark.sql.window import Window
import pandas as pd

# COMMAND ----------

faker=Faker()

faker.seed_instance(42)
Faker.seed(42)
random.seed(42)

# COMMAND ----------

# DBTITLE 1,NOTE: Shared datasets
# MAGIC %md
# MAGIC # NOTE
# MAGIC
# MAGIC * All data files generated in this notebook (patients, encounters, departments, transactions, claims, providers, etc.) are shared/applicable to **BOTH Hospital 1 and Hospital 2**.
# MAGIC * The same dataset is used for both hospitals; only the provider counts differ per hospital (see provider constants below).

# COMMAND ----------

#NUM_PATIENTS = 5000
NUM_LARGE_PATIENTS = 50000    
##NUM_ENCOUNTERS = 100000
NUM_DEPARTMENTS = 20
 
NUM_HOSPITAL_ENCOUNTERS = 10000
NUM_TRANSACTIONS = 10000
NUM_CLAIMS = 10000
 
NUM_PROVIDERS_HOSPITAL1 = 25
NUM_PROVIDERS_HOSPITAL2 = 30

# COMMAND ----------

encounter_types = [
    "Inpatient",
    "Outpatient",
    "Emergency",
    "Telemedicine",
    "Routine Checkup"
]
 
amount_types = [
    "Co-pay",
    "Insurance",
    "Self-pay",
    "Medicaid",
    "Medicare"
]
 
visit_types = [
    "Routine",
    "Follow-up",
    "Emergency",
    "Consultation"
]
 
line_of_business = [
    "Commercial",
    "Medicaid",
    "Medicare",
    "Self-Pay"
]
 
payors = [
    "Medicare",
    "Medicaid",
    "BlueCross",
    "Aetna",
    "UnitedHealthcare"
]
 
claim_statuses = [
    "Pending",
    "Approved",
    "Rejected",
    "Paid",
    "Denied"
]
 
payor_types = [
    "Government",
    "Private",
    "Self-pay"
]
 
specializations = [
    "Cardiology",
    "Neurology",
    "Orthopedics",
    "General Surgery",
    "Pediatrics",
    "Radiology",
    "Dermatology",
    "Oncology",
    "Anesthesiology",
    "Emergency Medicine",
    "Psychiatry"
]

# COMMAND ----------

##CPT AND ICT CODE 


 
icd_codes = [
    f"I{random.randint(10, 99)}.{random.randint(0, 9)}"
    for _ in range(100)
]

print(icd_codes)

root='/Volumes/faker/emr_data/emr_files'

# COMMAND ----------

def write_to_csv_hospi1(dataframe,filename,out_dir=root):
    path=f"{out_dir}/{filename}.csv"
    dataframe.toPandas().to_csv(path,index=False)
    print(f"Instered {dataframe.count} in the path {path}")

def write_to_csv_hospi2(dataframe,filename,out_dir=root):
    path=f"{out_dir}/{filename}.csv"
    dataframe.toPandas().to_csv(path,index=False)
    print(f"Instered {dataframe.count} in the path {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Patients File Generation For Both Hospitals

# COMMAND ----------

large_patients=[]

faker = Faker("en_IN")
gender=['Male','Female']

for i in range(1, NUM_LARGE_PATIENTS + 1):
    PatientID = f"HOSP{i:06d}"
    Gender = random.choice(gender)
    FirstName = faker.first_name_male() if Gender == "Male" else faker.first_name_female()
    LastName = faker.last_name()
    SSN = faker.ssn()
    PhoneNumber = faker.phone_number()
    DOB=faker.date_of_birth(minimum_age=0,maximum_age=100)
    Address=faker.address().replace("\n", ", ")
    ModifiedDate=faker.date_this_decade(before_today=True, after_today=False)
    large_patients.append((PatientID,FirstName,LastName,SSN,PhoneNumber,Gender,DOB,Address,ModifiedDate))

##Custom Schema 

large_schema='''
PatientID string,
FirstName string,
LastName string,
SSN string,
PhoneNumber string,
Gender string,
DOB date,
Address string,
ModifiedDate date
'''

large_patients_df_hospi1=spark.createDataFrame(large_patients,large_schema)
large_patients_df_hospi2=spark.createDataFrame(large_patients,large_schema)


# COMMAND ----------

# CSV write moved to end of notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Departments File Generation

# COMMAND ----------

departments=[]
department_name=[

"Cardiology",
    "Neurology",
    "Orthopedics",
    "General Surgery",
    "Pediatrics",
    "Radiology",
    "Dermatology",
    "Oncology",
    "Anesthesiology",
    "Emergency Medicine",
    "Psychiatry",
    "Gynecology",
    "Ophthalmology",
    "ENT",
    "Urology",
    "Nephrology",
    "Gastroenterology",
    "Pulmonology",
    "Endocrinology",
    "Rheumatology"

]

faker=Faker("en_IN")
NUM_DEPARTMENTS = len(department_name)
for i in range(1, NUM_DEPARTMENTS + 1):
    DeptID=f"DEPT{i:03d}"
    Name=department_name[i-1]
    departments.append((DeptID,Name))
departments_schema='''
DeptID string,
Name string
'''
departments_df_hospi1=spark.createDataFrame(departments,departments_schema)
departments_df_hospi2=spark.createDataFrame(departments,departments_schema)




# COMMAND ----------

display(departments_df_hospi1.count())

# COMMAND ----------

# CSV write moved to end of notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### CPT CODE FILE GENERATION

# COMMAND ----------

# ------------------------------------------------------------------
# Procedures File Generation For Both Hospitals
# Columns: ProcedureCode (3-digit random number), CPTCode (from the
#          cpt_codes list already defined earlier), ProcedureDescription
# ------------------------------------------------------------------
cpt_codes = [
    str(random.randint(10000, 99999))
    for _ in range(1000)
]

procedure_descriptions = [
    "General Consultation",
    "Physical Examination",
    "Blood Panel Test",
    "X-Ray Imaging",
    "MRI Scan",
    "CT Scan",
    "Ultrasound",
    "Echocardiogram",
    "Biopsy",
    "Vaccination",
    "Wound Dressing",
    "Suturing",
    "Catheterization",
    "Dialysis Session",
    "Chemotherapy Session",
    "Physiotherapy Session",
    "Anesthesia Administration",
    "Minor Surgery",
    "Major Surgery",
    "Health Screening"
]
cpt_code_category_map = {
    "General Consultation": "CON",
    "Physical Examination": "EXM",
    "Blood Panel Test": "BLD",
    "X-Ray Imaging": "XRY",
    "MRI Scan": "MRI",
    "CT Scan": "CTS",
    "Ultrasound": "ULT",
    "Echocardiogram": "ECO",
    "Biopsy": "BIO",
    "Vaccination": "VAC",
    "Wound Dressing": "WND",
    "Suturing": "SUT",
    "Catheterization": "CAT",
    "Dialysis Session": "DIA",
    "Chemotherapy Session": "CHE",
    "Physiotherapy Session": "PHY",
    "Anesthesia Administration": "ANE",
    "Minor Surgery": "MIN",
    "Major Surgery": "MAJ",
    "Health Screening": "SCR"
}
# Only 900 unique 3-digit numbers exist (100-999), so cap the row count at 900
num_cpt_code=len(cpt_codes)

#print(num_cpt_code)



# COMMAND ----------

cpt_code_file = []

for i in range(1, num_cpt_code + 1):
    ProcedureCodeDescription = random.choice(procedure_descriptions)
    ProcedureCodeCategory = cpt_code_category_map[ProcedureCodeDescription]  #ProcedureCodeDescription---> KEy
    CPTCode = cpt_codes[i - 1]
    cpt_code_file.append((ProcedureCodeCategory, CPTCode, ProcedureCodeDescription))

cpt_code_schema='''
ProcedureCodeCategory string,
CPTCode string,
ProcedureCodeDescription string
'''
cpt_code_df_hospi=spark.createDataFrame(cpt_code_file,cpt_code_schema)
cpt_code_df_hospi2=spark.createDataFrame(cpt_code_file,cpt_code_schema)
#display(cpt_code_df_hospi1)


display(cpt_code_df_hospi.count())



# COMMAND ----------

# MAGIC %md
# MAGIC ### Providers File Generations 

# COMMAND ----------

DeptId=[row[0] for row in departments_df_hospi1]

# COMMAND ----------

#Hospital 1
DeptIds=[row[0] for row in departments]
providers_file_hosp1=[]
for i in range (1,NUM_PROVIDERS_HOSPITAL1+1):
    ProviderID=f"H1-PROV{i:04d}"
    FirstName=faker.first_name()
    LastName=faker.last_name()
    Specialization=random.choice(specializations)
    DeptId=random.choice(DeptIds)
    NPI=faker.numerify("##########")
    providers_file_hosp1.append((ProviderID,FirstName,LastName,Specialization,DeptId,NPI))

providers_file_hosp1_schema='''
ProviderID string,
FirstName string,
LastName string,
Specialization string,
DeptId string,
NPI string
'''
providers_df_hosp1=spark.createDataFrame(providers_file_hosp1,providers_file_hosp1_schema)
display(providers_df_hosp1)


# COMMAND ----------

#Hospital 2
DeptIds=[row[0] for row in departments]
providers_file_hosp2=[]
for i in range (1,NUM_PROVIDERS_HOSPITAL2+1):
    ProviderID=f"H2-PROV{i:04d}"
    FirstName=faker.first_name()
    LastName=faker.last_name()
    Specialization=random.choice(specializations)
    DeptId=random.choice(DeptIds)
    NPI=faker.numerify("##########")
    providers_file_hosp2.append((ProviderID,FirstName,LastName,Specialization,DeptId,NPI))

providers_file_hosp2_schema='''
ProviderID string,
FirstName string,
LastName string,
Specialization string,
DeptId string,
NPI string
'''
providers_df_hosp2=spark.createDataFrame(providers_file_hosp2,providers_file_hosp2_schema)
display(providers_df_hosp2)


# COMMAND ----------

## Store the Proiders Files



# COMMAND ----------

# MAGIC %md
# MAGIC ### Encounters FIle PREPARATION

# COMMAND ----------

provider_ids=[row[0] for row in providers_file_hosp2]
print(provider_ids)

# COMMAND ----------

cpt_codes=[row[1] for row in cpt_code_file]
patient_ids=[row[0] for row in large_patients]
dept_ids=[row[0] for row in departments]
provider_ids=[row[0] for row in providers_file_hosp1]
encunters_files_hospi1=[]
for i in range (1,NUM_HOSPITAL_ENCOUNTERS+1):
    EncounterID=f"H1-ENC{i:06d}"
    PatientID=random.choice(patient_ids)
    EncounterDate=faker.date_this_decade(before_today=True,after_today=False)
    EncounterType=random.choice(encounter_types)
    ProviderID=random.choice(provider_ids) 
    DepartmentID=random.choice(dept_ids)
    ProcedureCode=random.choice(cpt_codes)
    InsertedDate=faker.date_this_decade(before_today=True,after_today=False)
    ModifiedDate=faker.date_this_decade(before_today=True,after_today=False)
    encunters_files_hospi1.append((EncounterID,PatientID,EncounterDate,EncounterType,ProviderID,DepartmentID,ProcedureCode,InsertedDate,ModifiedDate))
encounters_file_schema='''
EncounterID string,
PatientID string,
EncounterDate string,
EncounterType string,
ProviderID string,
DepartmentID string,
ProcedureCode string,
InsertedDate string,
ModifiedDate string
'''
encounters_file_hospi1_df=spark.createDataFrame(encunters_files_hospi1,encounters_file_schema)
display(encounters_file_hospi1_df)


# COMMAND ----------

cpt_codes=[row[1] for row in cpt_code_file]
patient_ids=[row[0] for row in large_patients]
dept_ids=[row[0] for row in departments]
provider_ids=[row[0] for row in providers_file_hosp2]
encunters_files_hospi2=[]
for i in range (1,NUM_HOSPITAL_ENCOUNTERS+1):
    EncounterID=f"H2-ENC{i:06d}"
    PatientID=random.choice(patient_ids)
    EncounterDate=faker.date_this_decade(before_today=True,after_today=False)
    EncounterType=random.choice(encounter_types)
    ProviderID=random.choice(provider_ids) 
    DepartmentID=random.choice(dept_ids)
    ProcedureCode=random.choice(cpt_codes)
    InsertedDate=faker.date_this_decade(before_today=True,after_today=False)
    ModifiedDate=faker.date_this_decade(before_today=True,after_today=False)
    encunters_files_hospi2.append((EncounterID,PatientID,EncounterDate,EncounterType,ProviderID,DepartmentID,ProcedureCode,InsertedDate,ModifiedDate))
encounters_file_schema='''
EncounterID string,
PatientID string,
EncounterDate string,
EncounterType string,
ProviderID string,
DepartmentID string,
ProcedureCode string,
InsertedDate string,
ModifiedDate string
'''
encounters_file_hospi2_df=spark.createDataFrame(encunters_files_hospi2,encounters_file_schema)
display(encounters_file_hospi2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claim File Generation
# MAGIC

# COMMAND ----------


payors = ["Medicare", "Medicaid", "BlueCross", "Aetna", "UnitedHealthcare"]

claim_statuses = ["Pending", "Approved", "Rejected", "Paid", "Denied"]

payor_types = ["Government", "Private", "Self-pay"]

payor_type_mapping = {
    "Medicare": "Government",
    "Medicaid": "Government",
    "BlueCross": "Private",
    "Aetna": "Private",
    "UnitedHealthcare": "Private"
}

# COMMAND ----------

from builtins import round
patient_ids=[row[0] for row in large_patients]
dept_ids=[row[0] for row in departments]
provider_ids=[row[0] for row in providers_file_hosp1]
encounter_ids=[row[0] for row in encunters_files_hospi1]
claim_file_hospi1=[]
for i in range(1,NUM_CLAIMS+1):
    ClaimID=f"CLAIM{i:06d}"
    PatientID=random.choice(patient_ids)
    EncounterID=random.choice(encounter_ids)
    ProviderID=random.choice(provider_ids)
    DeptID=random.choice(dept_ids)
    ServiceDate =faker.date_this_decade(before_today=True,after_today=False)
    ClaimDate=faker.date_this_decade(before_today=ServiceDate,after_today=False)
    PayorID=random.choice(payors)
    PayorType = payor_type_mapping[PayorID]
    ClaimAmount= round(random.uniform(1000, 50000), 2)
    CopayPercentage=round(random.uniform(5, 50), 2)  ##Percentage Value
    Copay = round(ClaimAmount * CopayPercentage / 100, 2)
    Deductible = round(random.uniform(0, 5000), 2)
    Coinsurance = round(random.uniform(0, 30), 2)
    ClaimStatus = random.choice(claim_statuses)
    if ClaimStatus in ["Pending", "Rejected", "Denied"]:
        PaidAmount = 0.00
    else:  # Approved or Paid
        PaidAmount = round(ClaimAmount - Copay, 2)
    InsertDate=faker.date_this_decade(before_today=True, after_today=False)
    ModifiedDate=faker.date_this_decade(before_today=InsertDate, after_today=False)
    claim_file_hospi1.append((ClaimID,PatientID,EncounterID,ProviderID,DeptID,ServiceDate,ClaimDate,PayorID,ClaimAmount,PaidAmount,ClaimStatus,PayorType,Deductible,Coinsurance,Copay,InsertDate,ModifiedDate))
claim_file_schema='''
ClaimID string,
PatientID string,
EncounterID string,
ProviderID string,
DeptID string,
ServiceDate string,
ClaimDate string,
PayorID string,
ClaimAmount double,
PaidAmount double,
ClaimStatus string,
PayorType string,
Deductible double,
Coinsurance double,
Copay double,
InsertDate string,
ModifiedDate string
'''
claim_file_hospi1_df=spark.createDataFrame(claim_file_hospi1, claim_file_schema)
display(claim_file_hospi1_df)



# COMMAND ----------

from builtins import round
patient_ids=[row[0] for row in large_patients]
dept_ids=[row[0] for row in departments]
provider_ids=[row[0] for row in providers_file_hosp2]
encounter_ids=[row[0] for row in encunters_files_hospi2]
claim_file_hospi2=[]
for i in range(1,NUM_CLAIMS+1):
    ClaimID=f"CLAIM{i:06d}"
    PatientID=random.choice(patient_ids)
    EncounterID=random.choice(encounter_ids)
    ProviderID=random.choice(provider_ids)
    DeptID=random.choice(dept_ids)
    ServiceDate =faker.date_this_decade(before_today=True,after_today=False)
    ClaimDate=faker.date_this_decade(before_today=ServiceDate,after_today=False)
    PayorID=random.choice(payors)
    PayorType = payor_type_mapping[PayorID]
    ClaimAmount= round(random.uniform(1000, 50000), 2)
    CopayPercentage=round(random.uniform(5, 50), 2)  ##Percentage Value
    Copay = round(ClaimAmount * CopayPercentage / 100, 2)
    ClaimStatus = random.choice(claim_statuses)
    if ClaimStatus in ["Pending", "Rejected", "Denied"]:
        PaidAmount = 0.00
    else:  # Approved or Paid
        PaidAmount = round(ClaimAmount - Copay, 2)
    InsertDate=faker.date_this_decade(before_today=True, after_today=False)
    ModifiedDate=faker.date_this_decade(before_today=InsertDate, after_today=False)
    claim_file_hospi2.append((ClaimID,PatientID,EncounterID,ProviderID,DeptID,ServiceDate,ClaimDate,PayorID,ClaimAmount,PaidAmount,ClaimStatus,PayorType,Copay,InsertDate,ModifiedDate))
claim_file_schema='''
ClaimID string,
PatientID string,
EncounterID string,
ProviderID string,
DeptID string,
ServiceDate string,
ClaimDate string,
PayorID string,
ClaimAmount double,
PaidAmount double,
ClaimStatus string,
PayorType string,
Copay double,
InsertDate string,
ModifiedDate string
'''
claim_file_hospi2_df=spark.createDataFrame(claim_file_hospi2, claim_file_schema)
display(claim_file_hospi2_df)

# COMMAND ----------

patient_ids=[row[0] for row in large_patients]
provider_ids=[row[0] for row in providers_file_hosp1]
dept_ids=[row[0] for row in departments]
cpt_codes=[row[1] for row in cpt_code_file]
claim_ids=[row[0] for row in claim_file_hospi1]

trans_file_hospi1=[]
for i in range(1,NUM_TRANSACTIONS+1):
    EncounterID=f"TRANS{i:06d}"
    PatientID=random.choice(patient_ids)
    EncounterDate=faker.date_this_decade(before_today=True, after_today=False)
    EncounterType=random.choice(encounter_types)
    ProviderID=random.choice(provider_ids)
    DepartmentID=random.choice(dept_ids)
    ProcedureCode=random.choice(cpt_codes)
    InsertedDate=faker.date_this_decade(before_today=True, after_today=False)
    ModifiedDate=faker.date_this_decade(before_today=InsertedDate, after_today=False)
    ClaimID=random.choice(claim_ids)
    trans_file_hospi1.append((EncounterID,PatientID,EncounterDate,ClaimID,EncounterType,ProviderID,DepartmentID,ProcedureCode,InsertedDate,ModifiedDate))

encounters_file_schema='''
EncounterID string,
PatientID string,
EncounterDate string,
ClaimID string,
EncounterType string,
ProviderID string,
DepartmentID string,
ProcedureCode string,
InsertedDate string,
ModifiedDate string



'''
trans_file_hospi1_df=spark.createDataFrame(encounters_file_hospi1,encounters_file_schema)
display(trans_file_hospi1_df)

# COMMAND ----------

patient_ids=[row[0] for row in large_patients]
provider_ids=[row[0] for row in providers_file_hosp2]
dept_ids=[row[0] for row in departments]
cpt_codes=[row[1] for row in cpt_code_file]
claim_ids=[row[0] for row in claim_file_hospi2]

trans_file_hospi2=[]
for i in range(1,NUM_TRANSACTIONS+1):
    EncounterID=f"TRANS{i:06d}"
    PatientID=random.choice(patient_ids)
    EncounterDate=faker.date_this_decade(before_today=True, after_today=False)
    EncounterType=random.choice(encounter_types)
    ProviderID=random.choice(provider_ids)
    DepartmentID=random.choice(dept_ids)
    ProcedureCode=random.choice(cpt_codes)
    InsertedDate=faker.date_this_decade(before_today=True, after_today=False)
    ModifiedDate=faker.date_this_decade(before_today=InsertedDate, after_today=False)
    ClaimID=random.choice(claim_ids)
    trans_file_hospi2.append((EncounterID,PatientID,EncounterDate,ClaimID,EncounterType,ProviderID,DepartmentID,ProcedureCode,InsertedDate,ModifiedDate))

encounters_file_schema='''
EncounterID string,
PatientID string,
EncounterDate string,
ClaimID string,
EncounterType string,
ProviderID string,
DepartmentID string,
ProcedureCode string,
InsertedDate string,
ModifiedDate string



'''
trans_file_hospi2_df=spark.createDataFrame(trans_file_hospi2,encounters_file_schema)
display(trans_file_hospi2_df)

# COMMAND ----------

# DBTITLE 1,Write all CSVs
### Write all generated files to CSV at the end
write_to_csv_hospi1(large_patients_df_hospi1, "patients_hospital1"); write_to_csv_hospi2(large_patients_df_hospi2, "patients_hospital2"); write_to_csv_hospi1(departments_df_hospi1, "departments_hospi1"); write_to_csv_hospi2(departments_df_hospi2, "departments_hospi2"); write_to_csv_hospi1(cpt_code_df_hospi, "cpt_code_hospi1"); write_to_csv_hospi2(cpt_code_df_hospi2, "cpt_code_hospi2"); write_to_csv_hospi1(providers_df_hosp1, "providers_hospi1"); write_to_csv_hospi2(providers_df_hosp2, "providers_hospi2"); write_to_csv_hospi1(encounters_file_hospi1_df, "encounters_hospi1"); write_to_csv_hospi2(encounters_file_hospi2_df, "encounters_hospi2"); write_to_csv_hospi1(claim_file_hospi1_df, "claims_hospi1"); write_to_csv_hospi2(claim_file_hospi2_df, "claims_hospi2"); write_to_csv_hospi1(trans_file_hospi1_df, "transactions_hospi1"); write_to_csv_hospi2(trans_file_hospi2_df, "transactions_hospi2")
