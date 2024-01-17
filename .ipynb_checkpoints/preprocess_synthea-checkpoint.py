def load_synthea():

    import pandas as pd

    patient_df = pd.read_csv('patients.csv')
    allergy_df = pd.read_csv('allergies.csv')
    careplan_df = pd.read_csv('careplans.csv')
    conditions_df = pd.read_csv('conditions.csv')
    device_df = pd.read_csv('devices.csv')
    encounter_df = pd.read_csv('encounters.csv')
    imaging_study_df = pd.read_csv('imaging_studies.csv')
    immunization_df = pd.read_csv('immunizations.csv')
    medication_df = pd.read_csv('medications.csv')
    observation_df = pd.read_csv('observations.csv')
    procedure_df = pd.read_csv('procedures.csv')

    #clean dfs
    patient_df = patient_df.rename(columns={"Id":"PATIENT", "DEATHDATE":"DEAD"})
    patient_df = patient_df[['PATIENT', 'BIRTHDATE', 'DEAD','FIRST', 'LAST', 'MARITAL', 'RACE','GENDER']].copy()
    
    allergy_df = allergy_df.rename(columns={"START":"DATE"})
    allergy_df = allergy_df.drop(columns=["STOP"])
    
    careplan_df = careplan_df.rename(columns={"START":"DATE"})
    careplan_df = careplan_df.drop(columns=["STOP"])
    
    conditions_df = conditions_df.rename(columns={"START":"DATE"})
    conditions_df = conditions_df.drop(columns=["STOP"])
    
    device_df = device_df.rename(columns={"START":"DATE"})
    device_df = device_df.drop(columns=["STOP"])
    
    encounter_df = encounter_df.rename(columns={"Id":"ENCOUNTER", "START":"DATE"})
    encounter_df = encounter_df[['ENCOUNTER','DATE', 'PATIENT', 'ENCOUNTERCLASS', 'CODE', 'DESCRIPTION', 'REASONCODE', 'REASONDESCRIPTION']].copy()
    
    imaging_study_df = imaging_study_df.drop(columns=["Id", "SERIES_UID", "INSTANCE_UID", "SOP_DESCRIPTION"])
    
    immunization_df = immunization_df.drop(columns=["BASE_COST"])
    
    medication_df = medication_df.rename(columns={"START":"DATE"})
    medication_df = medication_df[['DATE','PATIENT', 'ENCOUNTER', 'CODE', 'DESCRIPTION', 'REASONCODE','REASONDESCRIPTION']].copy()
    
    procedure_df = procedure_df.drop(columns=["BASE_COST", "STOP"])
    procedure_df = procedure_df.rename(columns={"START":"DATE"})

    return patient_df, allergy_df, careplan_df, conditions_df, device_df, encounter_df, imaging_study_df, immunization_df, medication_df, observation_df, procedure_df
                   

def generate_cohort(df, *patient_sets):

    case_patients = patient_sets[0].copy()
    for s in patient_sets[1:]:
        case_patients.intersection_update(s)

    print(f"Total patients meeting criteria: {len(case_patients)}")
    print(f"Total patients not meeting criteria: {len(df[~df['PATIENT'].isin(case_patients)])}")

    return list(case_patients)

def filter_cohort_data(cohort, case_patients, patient_df, allergy_df, careplan_df, conditions_df, device_df, encounter_df, imaging_study_df, immunization_df, medication_df, observation_df, procedure_df):

    """filter synthea files by cohort membership and clean
    """

    #filter by cohort membership
    patient_df = patient_df[patient_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    allergy_df = allergy_df[allergy_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    careplan_df = careplan_df[careplan_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    conditions_df = conditions_df[conditions_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    device_df = device_df[device_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    encounter_df = encounter_df[encounter_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    imaging_study_df = imaging_study_df[imaging_study_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    immunization_df = immunization_df[immunization_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    medication_df = medication_df[medication_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    observation_df = observation_df[observation_df["PATIENT"].isin(cohort)].reset_index(drop=True)
    procedure_df = procedure_df[procedure_df["PATIENT"].isin(cohort)].reset_index(drop=True)

    #flag case wing in patient list
    patient_df["CASE"] = 0
    patient_df.loc[patient_df['PATIENT'].isin(case_patients), "CASE"] = 1

    def string_to_datetime(date_string):

        """synthea dates given as string
        return datetime object"""
    
        try:
            date_string = date_string[:10]
            return datetime.strptime(date_string, '%Y-%m-%d').date()
        except:
            return date_string
    #transform date strings to datetime
    patient_df["BIRTHDATE"] = patient_df["BIRTHDATE"].apply(lambda x:string_to_datetime(x))
    patient_df["DEAD"] = patient_df["DEAD"].apply(lambda x:string_to_datetime(x))
    allergy_df["DATE"] = allergy_df["DATE"].apply(lambda x:string_to_datetime(x))
    careplan_df["DATE"] = careplan_df["DATE"].apply(lambda x:string_to_datetime(x))
    conditions_df["DATE"] = conditions_df["DATE"].apply(lambda x:string_to_datetime(x))
    device_df["DATE"] = device_df["DATE"].apply(lambda x:string_to_datetime(x))
    encounter_df["DATE"] = encounter_df["DATE"].apply(lambda x:string_to_datetime(x))
    imaging_study_df["DATE"] = imaging_study_df["DATE"].apply(lambda x:string_to_datetime(x))
    immunization_df["DATE"] = immunization_df["DATE"].apply(lambda x:string_to_datetime(x))
    medication_df["DATE"] = medication_df["DATE"].apply(lambda x:string_to_datetime(x))
    observation_df["DATE"] = observation_df["DATE"].apply(lambda x:string_to_datetime(x))
    procedure_df["DATE"] = procedure_df["DATE"].apply(lambda x:string_to_datetime(x))

    patient_birth_dict = dict(zip(patient_df["PATIENT"], patient_df["BIRTHDATE"]))

    def age_at_event(event, patient):
    
        """return age of patient at event in years
        """
    
        birthdate = patient_birth_dict[patient]
    
        try:
            delta = event - birthdate
        except:
            delta = datetime.now().date() - birthdate
    
        return math.floor(delta.days/365.25)

    #add age at event
    patient_df.insert(3, "AGE", patient_df[["DEAD", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    #dead as bool
    patient_df["DEAD"] = patient_df["DEAD"].fillna(0)
    patient_df.loc[patient_df['DEAD'] != 0, 'DEAD'] = 1
    
    allergy_df.insert(3, "AGE", allergy_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    careplan_df.insert(3, "AGE", careplan_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    conditions_df.insert(3, "AGE", conditions_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    device_df.insert(3, "AGE", device_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    encounter_df.insert(3, "AGE", encounter_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    imaging_study_df.insert(3, "AGE", imaging_study_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    immunization_df.insert(3, "AGE", immunization_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    medication_df.insert(3, "AGE", medication_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    observation_df.insert(3, "AGE", observation_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))
    procedure_df.insert(3, "AGE", procedure_df[["DATE", "PATIENT"]].apply(lambda x:age_at_event(*x), axis=1))

    return patient_df, allergy_df, careplan_df, conditions_df, device_df, imaging_study_df, immunization_df, medication_df, observation_df, procedure_df


                     
