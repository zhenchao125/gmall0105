package com.atguigu.dw.gmall0105publisher.controller

import com.atguigu.dw.gmall0105publisher.service.PublisherService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestParam, RestController}

@RestController
class PublisherController {
    @Autowired
    var publishService: PublisherService = _
    @GetMapping(Array("/realtime-total"))
    def getRealtimeTotalDau(@RequestParam date: String) ={
        val total: Long = publishService.getDauTotal(date)
        total
    }
}
