import threading
import time
import pandas as pd
import alg_adx, alg_ao, alg_aroon, alg_bbands, alg_cci, alg_coppock, alg_ema, alg_kc, alg_kst, alg_macd, alg_rsi, alg_rvi, alg_sma, alg_st
import alg_stoch, alg_tsi, alg_Willr
# import executor

def thread_manager(df, taskparameters, import_file_names, mydb):
    print("start....", taskparameters, import_file_names)
    max_allow_thread = 1
    semaphore = threading.Semaphore(max_allow_thread)
    # # taskparameters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K']
    # # import_file_names = ["Thread_A.callme", "Thread_B.callme", "Thread_C.callme", "Thread_D.callme", "Thread_E.callme", "Thread_F.callme"]
    # df = 
    threads = []    
    for i in range(0, len(taskparameters)): 
        params = taskparameters[i]
        fullName = import_file_names[i]
        print(fullName, params)
        if '.' in fullName:
            class_name, method_name = fullName.split('.')
            # print(df.columns)
            # thread = threading.Thread(target=getattr(globals()[class_name], method_name), args=(df, params, semaphore))
            thread = threading.Thread(target=my_caller, args=(df, class_name, params, mydb, semaphore))
            thread.start()
            threads.append(thread)


    for thread in threads:
        thread.join()

    print("task completed!")


def my_caller(df, class_name, params, mydb, semaphore):
    try:
        if class_name == "alg_adx":
            alg_adx.adx_buy_sell(df, params)
        elif class_name == "alg_ao":
            alg_ao.ao_buy_sell(df, params, mydb)
        elif class_name == "alg_aroon":
            alg_aroon.aroon_buy_sell(df, params)
        elif class_name == "alg_bbands":
            alg_bbands.bbands_buy_sell(df, params)
        elif class_name == "alg_cci":
            alg_cci.cci_buy_sell(df, params)
        elif class_name == "alg_coppock":
            alg_coppock.coppock_buy_sell(df, params)
        elif class_name == "alg_ema":
            alg_ema.ema_buy_sell(df, params)
        elif class_name == "alg_kc":
            alg_kc.kc_buy_sell(df, params)
        elif class_name == "alg_kst":
            alg_kst.kst_buy_sell(df, params)
        elif class_name == "alg_macd":
            alg_macd.macd_buy_sell(df, params)
        elif class_name == "alg_rsi":
            alg_rsi.rsi_buy_sell(df, params)
        elif class_name == "alg_rvi":
            alg_rvi.rvi_buy_sell(df, params)
        elif class_name == "alg_sma":
            alg_sma.sma_buy_sell(df, params)
        elif class_name == "alg_st":
            alg_st.st_buy_sell(df, params)
        elif class_name == "alg_stoch":
            alg_stoch.stoch_buy_sell(df, params)
        elif class_name == "alg_tsi":
            alg_tsi.tsi_buy_sell(df, params)
        elif class_name == "alg_Willr":
            alg_Willr.Willr_buy_sell(df, params)
        else :
            print("This Indicator is not available")
    except Exception as e:
        print(f"Error in {class_name}: {str(e)}")
    finally:
        semaphore.release() # Release the semaphore to allow another thread to start

