package com.saurabh.config;

import com.saurabh.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {

    @Override
    public Customer process(Customer customer) throws Exception {
        if(customer.getId()<1001) {
            return customer;
        }else{
            return null;
        }
    }
}
