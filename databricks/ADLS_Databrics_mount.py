#reference 
#dbutils.fs.mount( source = 'wasbs://<conatiner-name>@<storage-account-name>.blob.core.windows.net', 
                 mount_point= '<mount-point-name>', extra_configs ={'fs.azure.sas.<conatiner-name>.<storage-account-name>.blob.core.windows.net':'<SAS-Token>'})

#mounting adls with databticks for bronze layer container

dbutils.fs.mount( source = 'wasbs://bronze@kc123.blob.core.windows.net', 
                 mount_point= '/mnt/kc12', extra_configs ={'fs.azure.sas.bronze.kc123.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-16T14:51:59Z&st=2025-01-16T06:51:59Z&spr=https&sig=qipvQ%2FYz9OAAF3ZV8KqKBxmOZ6%2Bx3ogr9AKx%2BTQlPMM%3D'})


#mounting adls with databticks for silver layer container

dbutils.fs.mount( source = 'wasbs://silver@kc123.blob.core.windows.net', 
                 mount_point= '/mnt/silver/kc12', extra_configs ={'fs.azure.sas.silver.kc123.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-17T02:09:55Z&st=2025-01-16T18:09:55Z&spr=https&sig=3PFo8OhhLwpkeAAqLZlUexTQyqwDe0%2B%2BDmctosHS8Ug%3D'})


#mounting adls with databticks for gold layer container

dbutils.fs.mount( source = 'wasbs://gold@kc123.blob.core.windows.net', 
                 mount_point= '/mnt/gold/kc12', extra_configs ={'fs.azure.sas.gold.kc123.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-19T20:15:35Z&st=2025-01-17T12:15:35Z&spr=https&sig=hHshdAsZ8Iw07e91peukoTTAiK27MtrfYPTuuffPf%2B0%3D'})

