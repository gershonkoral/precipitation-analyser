import netCDF4 as nc
import numpy as np
from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
global_rank = comm.Get_rank()

# MPI handler class


class MPIMessenger():
    def __init__(self):
        from mpi4py import MPI
        self._mpi = MPI
        self._mpi_comm = self._mpi.COMM_WORLD
        self._mpi_size = self._mpi.COMM_WORLD.Get_size()
        self._mpi_rank = self._mpi.COMM_WORLD.Get_rank()
        self._is_master = (self._mpi_rank == 0)

    def partition(self, global_list):
        local_list = global_list[self._mpi_rank::self._mpi_size]
        return local_list


def avg_MPI(precip_dataset):
    mpi_obj = MPIMessenger()
    avg = 0
    rank = mpi_obj._mpi_rank
    if mpi_obj._is_master:
        for i in range(1, mpi_obj._mpi_size):
            avg = np.mean(precip_dataset)
            mpi_obj._mpi_comm.Send(avg, dest=i, tag=1)
    else:
        avg = np.empty(precip_dataset.size)
        mpi_obj._mpi_comm.Recv(avg, source=0, tag=1)

    return avg


def sort_MPI(precip_dataset):

    mpi_obj = MPIMessenger()
    rank = mpi_obj._mpi_rank
    #print("Start sort proc rank: ",rank, "mpi obj size =  ",mpi_obj._mpi_size)
    if mpi_obj._is_master:
        for i in range(1, mpi_obj._mpi_size):
            #print("Sorting proc rank: ",rank, "i = ",i)
            precip_dataset = np.sort(precip_dataset, axis=None)
            mpi_obj._mpi_comm.Send(precip_dataset, dest=i, tag=1)
    else:
        #print("Sorting proc rank: ",rank)
        precip_dataset = np.empty(precip_dataset.size)
        mpi_obj._mpi_comm.Recv(precip_dataset, source=0, tag=1)

    return precip_dataset


def median_MPI(precip_dataset):
    mpi_obj = MPIMessenger()
    rank = mpi_obj._mpi_rank
    median = 0
    if mpi_obj._is_master:
        for i in range(1, mpi_obj._mpi_size):
            median = np.median(precip_dataset, axis=None)
            mpi_obj._mpi_comm.Send(median, dest=i, tag=1)
    else:
        median = np.empty(precip_dataset.size)
        mpi_obj._mpi_comm.Recv(median, source=0, tag=1)

    return median


def quartile_(precip_dataset, no_quartile):
    mpi_obj = MPIMessenger()
    rank = mpi_obj._mpi_rank
    quartile_value = 0
    if mpi_obj._is_master:
        for i in range(1, mpi_obj._mpi_size):
            quartile_value = np.percentile(
                precip_dataset, no_quartile, interpolation='midpoint')
            mpi_obj._mpi_comm.Send(quartile_value, dest=i, tag=1)
    else:
        quartile_value = np.empty(precip_dataset.size)
        mpi_obj._mpi_comm.Recv(quartile_value, source=0, tag=1)

    return quartile_value


# Driver code
if(__name__ == '__main__'):

    if(global_rank == 0):

        #print("master process ",global_rank," initializing data files and determining data size")
        # for resolution = 0.5 files are 1982-2016
        # for resolution = 1   files are 1982-2018

        start_year = 1982
        end_year = 2018
        num_files = end_year-start_year+1
        years = str(start_year)+", "  # String of years included

        resolution_code = 0  # 0 = 10,  1 = 05 By default use 1 degree resolution
        if(resolution_code == 0):
            # 1 degree resolution dataset
            res1 = "v2020"
            res2 = "v2020_10"

        else:
            # 0.5 degree resolution dataset
            res1 = "V2018_05"
            res2 = "v2018_05"

        dayStart = 1
        dayEnd = 365
        latStart = 1
        latEnd = 90
        lonStart = 1
        lonEnd = 360

        fn = '/data/full_data_daily_' + \
            str(res1)+'/full_data_daily_'+str(res2)+'_'+str((start_year))+'.nc'
        ds = nc.Dataset(fn)
        precip_data = (ds['precip'][:])[
            dayStart:dayEnd, latStart:latEnd, lonStart:lonEnd]  # day of year lat long

        for i in range(1, num_files):
            years += str((start_year+i))+", "
            fn = '/data/full_data_daily_' + \
                str(res1)+'/full_data_daily_'+str(res2) + \
                '_'+str((start_year+i))+'.nc'
            ds = nc.Dataset(fn)
            # day of year lat long
            precip_data += (ds['precip'][:])[dayStart:dayEnd,
                                             latStart:latEnd, lonStart:lonEnd]

        #precip_data = np.ma.masked_equal(precip_data,0)    ### Masks zero values ###
        data = precip_data.compressed()  # Masks null values ###

        dataSize = data.size

    else:
        #print("sub process ",global_rank," initializing data size")
        dataSize = None

    # broadcast numData and allocate array on other ranks:
    dataSize = comm.bcast(dataSize, root=0)

    if global_rank != 0:
        #print("sub process ",global_rank," initializing data variable")
        data = np.empty(dataSize)

    # broadcast the array from rank 0 to all others
    comm.bcast(data, root=0)

    # All Processes
    #print("process ",global_rank," broadcast complete and beginning calculations")
    startTime = time.perf_counter()
    mean = avg_MPI(data)
    endTime = time.perf_counter()
    meanTime = endTime - startTime

    data = sort_MPI(data)

    startTime = time.perf_counter()
    median = median_MPI(data)
    endTime = time.perf_counter()
    medianTime = endTime - startTime

    startTime = time.perf_counter()
    upper_quartile = quartile_(data, 75)
    endTime = time.perf_counter()
    UQTime = endTime - startTime

    startTime = time.perf_counter()
    lower_quartile = quartile_(data, 25)
    endTime = time.perf_counter()
    LQTime = endTime - startTime

    startTime = time.perf_counter()
    maximum = data.max()
    endTime = time.perf_counter()
    maxTime = endTime - startTime

    startTime = time.perf_counter()
    minimum = data.min()
    endTime = time.perf_counter()
    minTime = endTime - startTime

    if(global_rank == 0):

        #print("master process ",global_rank," printing results and terminating")

        print("Box and Whisker Plot Parameters \n" +
              "For Latitudes ", latStart, " - ", latEnd,
              "\nFor ", start_year, " - ", end_year,
              "\nResolution = ", res2[6:8], "\n")
        print("Mean: \t\t\t", mean, "\t\t Time taken: \t", meanTime)
        print("Median: \t\t", median, "\t\t\t Time taken: \t", medianTime)
        print("Upper Quartile: \t", upper_quartile, "\t Time taken: \t", UQTime)
        print("Lower Quartile: \t", lower_quartile,
              "\t\t\t Time taken: \t", LQTime)
        print("Maximum: \t\t", maximum, "\t\t Time taken: \t", maxTime)
        print("Minimum: \t\t", minimum, "\t\t\t Time taken: \t", minTime)

    # else:
        #print("sub process ",global_rank," terminating")

MPI.Finalize()
