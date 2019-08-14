from jskafka.das_fft import DasFft
import unittest
import time
import numpy as np


class TestConsumer(unittest.TestCase):

    def test_amplitudes_fft(self):

        amplitudes= np.random.rand(5000,10000)
        #amplitudes = np.random.rand(10000)

        dy = time.clock()

        das_fft = DasFft()
        fft = das_fft.amplitudes_fft(amplitudes)
        dx = time.clock()
        print(f'Time used {dx - dy}')
        self.assertEqual(amplitudes.__len__(), fft.__len__())


    def test_amplitudes_list_fft(self):

        amplitudesList= np.random.rand(100,10000)


        dy = time.clock()

        das_fft = DasFft()
        fft = das_fft.amplitudes_list_fft(amplitudesList)
        dx = time.clock()
        print(f'Time used {dx - dy}')
        #self.assertEqual(amplitudesList.__len__(), fft.__len__())


if __name__ == '__main__':
    unittest.main()

