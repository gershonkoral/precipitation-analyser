import netCDF4 as nc
import numpy as np

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
    if mpi_obj._is_master:
        for i in range(1, mpi_obj._mpi_size):
            precip_dataset = np.sort(precip_dataset, axis=None)
            mpi_obj._mpi_comm.Send(precip_dataset, dest=i, tag=1)
    else:
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
    fn = 'full_data_daily_v2020_10_2019.nc'
    ds = nc.Dataset(fn)

    precip_northern_hemisphere = (ds['precip'][:])[1:365, 91:180, 1:360]
    precip_northern_hemisphere = precip_northern_hemisphere.compressed()

    print("Mean: ", avg_MPI(precip_northern_hemisphere))

    precip_northern_hemisphere = sort_MPI(precip_northern_hemisphere)

    print("Median: ", median_MPI(precip_northern_hemisphere))
    print("Lower quartile: ", quartile_(precip_northern_hemisphere, 25))
    print("Upper quartile: ", quartile_(precip_northern_hemisphere, 75))
