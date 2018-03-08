#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by FFJ on 17-12-27

import logging

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s|PID:%(process)d|%(levelname)s: %(message)s',
                        level=logging.INFO)

    for i in range(50, 500, 50):
        print(i)