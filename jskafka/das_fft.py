import numpy as np
import concurrent.futures

class DasFft:

    def amplitudes_fft(self, amplitudes):
        return np.fft.fft(amplitudes)


    def amplitudes_list_fft(self, amplitudes_list):
        results = {}
        id = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=amplitudes_list.__sizeof__()) as executor:


            futures = {executor.submit(self.amplitudes_fft, p): p for p in amplitudes_list}
            for future in concurrent.futures.as_completed(futures):

                id = id + 1
                try:
                    data = future.result()
                except Exception as exc:
                    print(exc)
                else:
                    results[id] = data

        return results
