
import mysql.connector
import struct
import pymodbus.client.sync
import binascii
import time
import sys   

#start by creating a connection to the database.
mydb = mysql.connector.connect(
    host="localhost",
    user = "gateway019",
    password = "Kasania@019z",
    database = "gwrpi_19"
)

#used to execute statement to communicate with the MySQL database
mycursor = mydb.cursor()

def read_float_reg(client, basereg, unit=1) :
    resp = client.read_input_registers(basereg,2, unit=1)
    if resp == None :
        return None
    # according to spec, each pair of registers returned
    # encodes a IEEE754 float where the first register carries
    # the most significant 16 bits, the second register carries the 
    # least significant 16 bits.
    return struct.unpack('>f',struct.pack('>HH',*resp.registers)) 
 
def fmt_or_dummy(regfmt, val) :
    if val is None :
        return '.'*len(regfmt[2]%(0))
    return regfmt[2]%(val)

def main() : 
    regs = [
        # Symbol    Reg#  Format
        ( 'V',      0x00, '%6.2f' ), # Voltage [V]
        ( 'Curr',   0x06, '%6.2f' ), # Current [A]
        ( 'P[act]', 0x0c, '%6.0f' ), # Active Power ("Wirkleistung") [W]
        ( 'P[app]', 0x12, '%6.0f' ), # Apparent Power ("Scheinl.") [W]
        ( 'P[rea]', 0x18, '%6.0f' ), # Reactive Power ("Blindl.") [W]
        ( 'PF',     0x1e, '%6.3f' ), # Power Factor   [1]
        ( 'Phi',    0x24, '%6.1f' ), # cos(Phi)?      [1]
        ( 'Freq',   0x46, '%6.2f' )  # Line Frequency [Hz]
    ]
 
    # if client is set to odd or even parity, set stopbits to 1
    # if client is set to 'none' parity, set stopbits to 2

    cl = pymodbus.client.sync.ModbusSerialClient('rtu',
        port='/dev/ttyUSB0', baudrate=9600, parity='N',stopbits=2,
        timeout=0.125)

    def savetodatabase(voltage, current, active_power, apparent_power, reactive_power, power_factor, cos_phi, line_frequency): 
        sql = "INSERT INTO energy_usage (voltage, current, active_power, apparent_power, reactive_power, power_factor, cos_phi, line_frequency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (voltage, current, active_power, apparent_power, reactive_power, power_factor, cos_phi, line_frequency)
        mycursor.execute(sql, val)

        mydb.commit()
        
    N=0
    while True :
        N += 1 
        if N % 16 == 1 :
            print('#        '+(' '.join(['%-6s'%(x[0]) for x in regs])))
            print('#        '+(':'.join(['------'        for x in regs])))
 
        values = [ read_float_reg(cl, reg[1], unit=1) for reg in regs ]

        voltage = round(values[0][0], 2)
        current = round(values[1][0], 2)
        active_power = round(values[2][0], 2)
        apparent_power = round(values[3][0], 2)
        reactive_power = round(values[4][0], 2)
        power_factor = round(values[5][0], 2)
        cos_phi = round(values[6][0], 2)
        line_frequency = round(values[7][0], 2)

        #print(values)
        savetodatabase(voltage, current, active_power, apparent_power, reactive_power, power_factor, cos_phi, line_frequency)
        
        tstr=time.strftime('%H:%M:%S ')
        print(tstr+(' '.join([fmt_or_dummy(*t) for t in zip(regs, values)])))
        sys.stdout.flush()

if __name__ == '__main__' :
    main()   