package com.ww.es.service.impl;

import com.ww.es.mapper.HotelMapper;
import com.ww.es.pojo.Hotel;
import com.ww.es.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

}
