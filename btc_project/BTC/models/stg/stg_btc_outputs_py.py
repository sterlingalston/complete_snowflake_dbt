import pandas
import simplejson


def model(dbt, session):

    dbt.config(materialized="table", packages=["pandas", "simplejson"])

    df = dbt.ref("stg_btc").to_pandas()

    df["OUTPUTS"] = df["OUTPUTS"].apply(simplejson.loads)

    df_exploded = df.explode("OUTPUTS").reset_index(drop=True)

    df_outputs = pandas.json_normalize(df_exploded["OUTPUTS"])[["address", "value"]]

    df_final = pandas.concat([df_exploded.drop(columns="OUTPUTS"), df_outputs],axis=1)

    df_final = df_final[df_final["address"].notnull()]

    df_final.columns = [col.upper() for col in df_final.columns]

    return df
