import pymysql
from datetime import datetime
import time
import json

while (1):
    first_boot = 1
    now = datetime.now()
    try:
        connection_to_bank = 1
        # open connection db toko online
        try:
            connect_toko = pymysql.connect(host='sql110.epizy.com', user='epiz_31371652',
                                           password='lp6z3BgqWchE0lt', database='epiz_31371652_db_toko_online')
            cursor_toko = connect_toko.cursor()
        except:
            print("\n")
            print("||=================================================||")
            print("||                   NOTIFIKASI                    ||")
            print("||=================================================||")
            print("\n")
            print('-> Tidak dapat terhubung dengan basis data Toko Online')

        # open connection db bank
        try:
            connect_bank = pymysql.connect(host='localhost', user='bank',
                                           password='bankku19', database='db_bank')
            cursor_bank = connect_bank.cursor()
        except:
            print("||=================================================||")
            print("||                   NOTIFIKASI                    ||")
            print("||=================================================||")
            print("\n")
            print('-> Tidak dapat terhubung dengan basis data Bank')
            connection_to_bank = 0

        sql = "SELECT * FROM tb_invoice"
        cursor_toko.execute(sql)
        result = cursor_toko.fetchall()

        sql = "SELECT * FROM tb_integrasi"
        cursor_toko.execute(sql)
        integrasi = cursor_toko.fetchall()

        print("||=================================================||")
        print("||                   NOTIFIKASI                    ||")
        print("||=================================================||")
        print("||     PEMERIKSAAN DATA TOKO SEDANG BERLANGSUNG    ||")
        print("||=================================================||")
        print("\n")
        print("\n")
        print("transaksi len = %d | integrasi len = %d" %
              (len(result), len(integrasi)))

        # insert listener
        if(len(result) > len(integrasi)):
            print("\n")
            print("\n")
            print("||=================================================||")
            print("||                   NOTIFIKASI                    ||")
            print("||=================================================||")
            print("||          SEDANG TERJADI PENAMBAHAN DATA         ||")
            print("||=================================================||")
            for data in result:
                a = 0
                for data_integrasi in integrasi:
                    if(data[0] == data_integrasi[0]):
                        a = 1
                if (a == 0):
                    print("\n")
                    print("\n")
                    print("||=================================================||")
                    print("||                   NOTIFIKASI                    ||")
                    print("||=================================================||")
                    print("\n")
                    print("==> PENAMBAHAN DATA PADA ID = %s DI %s" %
                          (data[0], now))
                    print("\n")
                    print("\n")
                    val = (data[1], data[2], data[3], data[4])
                    insert_integrasi_toko = "INSERT INTO tb_integrasi (no_rekening,tgl_trx,total_transaksi, status)" \
                                            "VALUES(%s, %s, %s, %s)"
                    cursor_toko.execute(insert_integrasi_toko, val)
                    connect_toko.commit()

                    if(connection_to_bank == 1):
                        insert_integrasi_bank = "INSERT INTO tb_integrasi (no_rekening,tgl_trx,total_transaksi, status)" \
                                                "VALUES(%s, %s, %s, %s)"
                        cursor_bank.execute(insert_integrasi_bank, val)
                        connect_bank.commit()

                        insert_transaksi_bank = "INSERT INTO tb_invoice (no_rekening,tgl_trx,total_transaksi, status)" \
                                                "VALUES(%s, %s, %s, %s)"
                        cursor_bank.execute(insert_transaksi_bank, val)
                        connect_bank.commit()

        # delete listener
        if(len(result) < len(integrasi)):
            print("\n")
            print("\n")
            print("||=================================================||")
            print("||                   NOTIFIKASI                    ||")
            print("||=================================================||")
            print("||         SEDANG TERJADI PENGHAPUSAN DATA         ||")
            print("||=================================================||")
            print("\n")
            print("\n")
            for dataIntegrasi in integrasi:
                a = 0
                for data in result:
                    if(dataIntegrasi[0] == data[0]):
                        a = 1
                if (a == 0):
                    print("||=================================================||")
                    print("||                   NOTIFIKASI                    ||")
                    print("||=================================================||")
                    print("\n")
                    print("==> PENGHAPUSAN DATA PADA ID = %s DI %s --" %
                          (dataIntegrasi[0], now))
                    print("\n")
                    print("\n")
                    # delete row in tb_integrasi in db_toko
                    delete_integrasi_toko = "DELETE FROM tb_integrasi WHERE id_trx = '%s'" % (
                        dataIntegrasi[0])
                    cursor_toko.execute(delete_integrasi_toko)
                    connect_toko.commit()

                    if(connection_to_bank == 1):
                        delete_integrasi_bank = "DELETE FROM tb_integrasi WHERE id_trx = '%s'" % (
                            dataIntegrasi[0])
                        cursor_bank.execute(delete_integrasi_bank)
                        connect_bank.commit()
                        delete_transaksi_bank = "DELETE FROM tb_invoice WHERE id_trx = '%s'" % (
                            dataIntegrasi[0])
                        cursor_bank.execute(delete_transaksi_bank)
                        connect_bank.commit()

        # update listener
        if (result != integrasi):
            for data in result:
                for dataIntegrasi in integrasi:
                    if(data[0] == dataIntegrasi[0]):
                        if(data != dataIntegrasi):
                            print("\n")
                            print("\n")
                            print(
                                "||=================================================||")
                            print(
                                "||                   NOTIFIKASI                    ||")
                            print(
                                "||=================================================||")
                            print(
                                "||          SEDANG TERJADI PERUBAHAN DATA          ||")
                            print(
                                "||=================================================||")
                            print("\n")
                            print("\n")
                            print(
                                "||=================================================||")
                            print(
                                "||                   NOTIFIKASI                    ||")
                            print(
                                "||=================================================||")
                            print("\n")
                            print("==> PERUBAHAN DATA PADA ID = %s DI %s --" %
                                  (dataIntegrasi[0], now))
                            print("\n")
                            print("\n")
                            val = (data[1], data[2], data[3], data[4], data[0])
                            update_integrasi_toko = "update tb_integrasi set no_rekening = %s, tgl_trx = %s," \
                                                    "total_transaksi = %s, status = %s where id_trx = %s"
                            cursor_toko.execute(update_integrasi_toko, val)
                            connect_toko.commit()

                            if(connection_to_bank == 1):
                                update_integrasi_bank = "update tb_integrasi set no_rekening = %s, tgl_trx = %s," \
                                                        "total_transaksi = %s, status = %s where id_trx = %s"
                                cursor_bank.execute(update_integrasi_bank, val)
                                connect_bank.commit()

                                update_transaksi_bank = "update tb_invoice set no_rekening = %s,  tgl_trx = %s," \
                                                        "total_transaksi = %s, status = %s where id_trx = %s"
                                cursor_bank.execute(update_transaksi_bank, val)
                                connect_bank.commit()

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    # untuk delay
    time.sleep(2)
