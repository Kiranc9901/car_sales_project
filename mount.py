dbutils.fs.mount( source = 'wasbs://bronze@kc123.blob.core.windows.net', 
                 mount_point= '/mnt/kc12', extra_configs ={'fs.azure.sas.bronze.kc123.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-16T14:51:59Z&st=2025-01-16T06:51:59Z&spr=https&sig=qipvQ%2FYz9OAAF3ZV8KqKBxmOZ6%2Bx3ogr9AKx%2BTQlPMM%3D'})


