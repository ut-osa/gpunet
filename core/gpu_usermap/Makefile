obj-m += gpu_usermap.o

EXTRA_CFLAGS +=-I./
PWD  := $(shell pwd)
KVER := $(shell uname -r)
MODULES_DIR := /lib/modules/$(KVER)
KDIR := $(MODULES_DIR)/build
MODULE_DESTDIR := $(MODULES_DIR)/extra/
DEPMOD := /sbin/depmod

KERNEL_VER?=$(shell uname -r)
all:
	cat nv.symvers >> Module.symvers
	make -C $(KDIR) M=$(PWD) NOSTDINC_FLAGS="$(EXTRA_CFLAGS)" modules

clean:
	make -C $(KDIR)  M=$(PWD) clean

install:
	mkdir -p $(DESTDIR)/$(MODULE_DESTDIR);
	cp -f $(PWD)/gpu_usermap.ko $(DESTDIR)/$(MODULE_DESTDIR);
	if [ ! -n "$(DESTDIR)" ]; then $(DEPMOD) -r -ae $(KVER);fi;
	bash -c 'echo KERNEL==\"gpu_usermap\", NAME=\"gpu_usermap\", MODE=\"0666\" > /etc/udev/rules.d/91-gpu_usermap.rules'

uninstall:
	/bin/rm -f $(DESTDIR)/$(MODULE_DESTDIR)/gpu_usermap.ko
	if [ ! -n "$(DESTDIR)" ]; then $(DEPMOD) -r -ae $(KVER);fi;
