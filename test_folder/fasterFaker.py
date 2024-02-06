import json
from faker import Faker
import random
from datetime import datetime, timedelta
from bson import ObjectId
import pandas as pd
import os

fake = Faker()

def generate_fake_data(current_time,num_records: int):
    fake = Faker()
    data_list = []
    fixed_mail_sender = '"Sapia.ai" <notification@product.sapia.ai>'

    # List of possible template types
    template_types = [
        "RESEND_FI_INVITE", "TI_PDF_DOWNLOAD", "FI_NOT_COMPLETED_SECOND_REMINDER", 
        "SI_NOT_STARTED_SECOND_REMINDER", "EMAIL_RESEND_MFA", "SI_INVITE", 
        "FI_INVITE", "FI_NOT_COMPLETED_FIRST_REMINDER", "SI_NOT_STARTED_FIRST_REMINDER", 
        "FI_SUBMIT_INTERVIEW", "EMAIL_RESET_PASSWORD", "MI_SELF_REPEATER", 
        "FI_START_INTERVIEW", "FI_NOT_STARTED_SECOND_REMINDER", "SI_START_INTERVIEW", 
        "MI_REPORT", "MI_NON_ENGLISH", "FI_NOT_STARTED_FIRST_REMINDER", 
        "MI_GARBAGE", "SI_SUBMIT_INTERVIEW", "SI_NOT_COMPLETED_FIRST_REMINDER", 
        "EMAIL_MFA", "MI_TOO_SHORT", "CREATE_NEW_USER", "RESEND_SI_INVITE", 
        "SI_NOT_COMPLETED_SECOND_REMINDER"
    ]

    customer_ids = [
        "62f05e1f9e06e38427fad5ef", "641a9c68715cb361773f3a99", "64e4459aebad94fdf03be129",
        "619d07da52daba000e1da2a6", "6434b021715cb3f9a75d66a6", "64eff0cdebad943c7d4c18d1",
        "628b2502b1c0e55082219259", "62bdde2c73da275615cd1c55", "62e73a4d355e6f2dd53ba02b",
        "62d864a7db78d51ecd500948", "624e7ba07df24134958019cb", "64b9c478650e10521463f4af",
        "63f7f231ebd7001df2ea1d6d", "6566840e310cc3a6b746d3d1", "627890086c02617a2eed1bcd",
        "642e0de3715cb31b1756212e", "6139b7b29dbc58b7b632688c", "64c200ed6846082e87200551",
        "57a02bed3b5ef24805d5d254", "63c72d4e8cb82f05c7b093b6", "650cfeb7bb000481cfdeab4e",
        "62e73a34173f327fc27eb602", "624d22a9b363d5af192c2cf4", "63ea2db5f377091795c749c1",
        "64a61ff16fa9ac58092f310c", "62f49c83fd0ccc63f67c5ae2", "64bf4276650e10bc666a5919",
        "6230391e75f1934fb0f69e39", "62a1470d1ef036c89f443d56", "63047675b06a09cf0c06807b",
        "60795822cfc0a5001188429e", "644c0f059a350404f32b7e7a", "64a61fe26fa9acadc32f30f3",
        "64d219bec90d756850f2130b", "621f09c95e9c9824a934adcf", "63eca3b3f37709514dca55c1",
        "62993d86e3686a9257934906", "644c0ea6715cb33e3577954f", "62a93afb47b06c9573e27818",
        "644c0f1f9a350451ed2b7e96", "63c72d1701528c0410e17c7a", "6361a075c192ca1a7f1f9a09",
        "629611e457238a8a331f4767", "62a7d12d47b06c67a4e260be", "629407ea153443505aae3fdd",
        "64db618f6846084ddd3cf7d8", "64b4ab4c650e10ac155cd299"
    ]


    for i in range(num_records):
        record_time = current_time - timedelta(seconds=i * 2.5)
        fake_data = {
                'templateType': fake.random_element(template_types),
                'subject': fake.sentence(),
                'templateId': fake.uuid4(),
                'mailRecipient': fake.email(),
                'mailSender': fixed_mail_sender,
                'customerId': fake.random_element(customer_ids),
                'id': fake.uuid4(),
                'timestamp': record_time.isoformat(),
                'eventType': fake.word(),
                'ph_templateType': fake.word(),
                'masterId': fake.uuid4(),
                'mailMessageId': fake.uuid4()
            }
        data_list.append(fake_data)

    return data_list,record_time







def save_data_to_folder(base_folder, data):
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    first_timestamp = df.iloc[0]['timestamp']
    last_timestamp = df.iloc[-1]['timestamp']

    grouped = df.groupby(['customerId','year','month'])
    sub_dfs = {(customer_id,year,month): group for (customer_id,year,month), group in grouped}
    
    for key,value in sub_dfs.items():
        first_timestamp = value.iloc[0]['timestamp']
        last_timestamp = value.iloc[-1]['timestamp']
        customer_id = key[0]
        year = key[1]
        month = key[2]

        folder_path = os.path.join(base_folder,f"customer_id={customer_id}",f"year={year}", f"month={month}")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_path = os.path.join(folder_path, f"from{first_timestamp}_to{last_timestamp}.csv")
        value.to_csv(file_path, index=False)





if __name__ == "__main__":
    total_data_size = 2000000
    batch = 2000000
    folder = 'data-glue'
    current_time = datetime.now()
    while total_data_size > 0:
        data,current_time = generate_fake_data(current_time,batch)
        save_data_to_folder(folder, data)
        total_data_size -= batch
        print(f"Remaining data size: {total_data_size}")
        
        

    