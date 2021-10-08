# hpguppi_daq

To compile:
```
 $ cd src
 $ autoreconf -is
 $ ./configure --with-libsla=/usr/local/listen/lib --with-libcoherent_beamformer=/home/mruzinda/beamformer_workspace/lib
 $ make
 ```

NOTE: The coherent beamformer library is currently located in mruzinda's directory so to run this, ensure you have the right path to that library.
