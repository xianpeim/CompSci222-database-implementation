
#include "qe.h"

//Filter
Filter::Filter(Iterator *input, const Condition &condition) {
    iter = input;
    cond = condition;
    iter->getAttributes(attrs);
    resetIter();
    //based on condition. do filtering here on scan.
}

// #TODO: check if cond.bRhsIsAttr can be true.
RC Filter::getNextTuple(void *data) {
    RC rc = iter->getNextTuple(data);
    if(rc == QE_EOF) return rc;
    void* tmp_data = malloc(BUFFSIZE);
    int null_size = getActualByteForNullsIndicator(attrs.size());
    int offset = null_size;
    int null_val;
    memcpy(&null_val, data, null_size);
    for(int i=0; i< attrs.size(); i++){
        if(attrs[i].name != cond.lhsAttr and !(null_val & ((unsigned) 1 << (unsigned)(7-i)))){
            switch (attrs[i].type){
                case TypeInt:
                    offset += sizeof(int);
                    break;
                case TypeReal:
                    offset += sizeof(float);
                    break;
                case TypeVarChar:
                    int length;
                    memcpy(&length, (char*)data+offset, sizeof(int));
                    offset += sizeof(int)+length;
                    break;
            }
        } else{
            switch (attrs[i].type){
                case TypeInt:
                    memcpy(tmp_data, (char*)data+offset, sizeof(int));
                    break;
                case TypeReal:
                    memcpy(tmp_data, (char*)data+offset, sizeof(float));
                    break;
                case TypeVarChar:
                    int length;
                    memcpy(&length, (char*)data+offset, sizeof(int));
                    memcpy(tmp_data, (char*)data+offset, sizeof(int)+length);
                    break;
            }
        }
    }

    switch (cond.op){
        case LE_OP:
            if(cond.bRhsIsAttr){
                std::cerr << "has not implemented yet" << std::endl;
            }
            else{
                switch (cond.rhsValue.type){
                    case TypeInt:
                        int int_val;
                        memcpy(&int_val, tmp_data, sizeof(int));
                        int comp_int_val;
                        memcpy(&comp_int_val, cond.rhsValue.data, sizeof(int));
                        if(int_val <= comp_int_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeReal:
                        float float_val;
                        memcpy(&float_val, tmp_data, sizeof(float));
                        float comp_float_val;
                        memcpy(&comp_float_val, cond.rhsValue.data, sizeof(float));
                        if(float_val <= comp_float_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data, sizeof(int));
                        int comp_length;
                        memcpy(&comp_length, cond.rhsValue.data, sizeof(int));
                        if(length < comp_length) return rc;
                        if(length > comp_length) return getNextTuple(data);
                        if(memcmp((char*)tmp_data+sizeof(int), (char*)cond.rhsValue.data+sizeof(int), length) <= 0){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    default:{
                        std::cerr << "should never reach here" << std::endl;
                        free(tmp_data);
                        tmp_data = NULL;
                        return -11;
                    }
                }
            }

            break;

        case LT_OP:
            if(cond.bRhsIsAttr){
                std::cerr << "has not implemented yet" << std::endl;
            }
            else{
                switch (cond.rhsValue.type){
                    case TypeInt:
                        int int_val;
                        memcpy(&int_val, tmp_data, sizeof(int));
                        int comp_int_val;
                        memcpy(&comp_int_val, cond.rhsValue.data, sizeof(int));
                        if(int_val < comp_int_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeReal:
                        float float_val;
                        memcpy(&float_val, tmp_data, sizeof(float));
                        float comp_float_val;
                        memcpy(&comp_float_val, cond.rhsValue.data, sizeof(float));
                        if(float_val < comp_float_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data, sizeof(int));
                        int comp_length;
                        memcpy(&comp_length, cond.rhsValue.data, sizeof(int));
                        if(length < comp_length) return rc;
                        if(length > comp_length) return getNextTuple(data);
                        if(memcmp((char*)tmp_data+sizeof(int), (char*)cond.rhsValue.data+sizeof(int), length) < 0){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    default:{
                        std::cerr << "should never reach here" << std::endl;
                        free(tmp_data);
                        tmp_data = NULL;
                        return -11;
                    }
                }
            }

            break;

        case GE_OP:
            if(cond.bRhsIsAttr){
                std::cerr << "has not implemented yet" << std::endl;
            }
            else{
                switch (cond.rhsValue.type){
                    case TypeInt:
                        int int_val;
                        memcpy(&int_val, tmp_data, sizeof(int));
                        int comp_int_val;
                        memcpy(&comp_int_val, cond.rhsValue.data, sizeof(int));
                        if(int_val >= comp_int_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeReal:
                        float float_val;
                        memcpy(&float_val, tmp_data, sizeof(float));
                        float comp_float_val;
                        memcpy(&comp_float_val, cond.rhsValue.data, sizeof(float));
                        if(float_val >= comp_float_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data, sizeof(int));
                        int comp_length;
                        memcpy(&comp_length, cond.rhsValue.data, sizeof(int));
                        if(length > comp_length) return rc;
                        if(length < comp_length) return getNextTuple(data);
                        if(memcmp((char*)tmp_data+sizeof(int), (char*)cond.rhsValue.data+sizeof(int), length) >= 0){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    default:{
                        std::cerr << "should never reach here" << std::endl;
                        free(tmp_data);
                        tmp_data = NULL;
                        return -11;
                    }
                }
            }

            break;


        case GT_OP:
            if(cond.bRhsIsAttr){
                std::cerr << "has not implemented yet" << std::endl;
            }
            else{
                switch (cond.rhsValue.type){
                    case TypeInt:
                        int int_val;
                        memcpy(&int_val, tmp_data, sizeof(int));
                        int comp_int_val;
                        memcpy(&comp_int_val, cond.rhsValue.data, sizeof(int));
                        if(int_val > comp_int_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeReal:
                        float float_val;
                        memcpy(&float_val, tmp_data, sizeof(float));
                        float comp_float_val;
                        memcpy(&comp_float_val, cond.rhsValue.data, sizeof(float));
                        if(float_val > comp_float_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data, sizeof(int));
                        int comp_length;
                        memcpy(&comp_length, cond.rhsValue.data, sizeof(int));
                        if(length > comp_length) return rc;
                        if(length < comp_length) return getNextTuple(data);
                        if(memcmp((char*)tmp_data+sizeof(int), (char*)cond.rhsValue.data+sizeof(int), length) > 0){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    default:{
                        std::cerr << "should never reach here" << std::endl;
                        free(tmp_data);
                        tmp_data = NULL;
                        return -11;
                    }
                }
            }

            break;

        case EQ_OP:
            if(cond.bRhsIsAttr){
                std::cerr << "has not implemented yet" << std::endl;
            }
            else{
                switch (cond.rhsValue.type){
                    case TypeInt:
                        int int_val;
                        memcpy(&int_val, tmp_data, sizeof(int));
                        int comp_int_val;
                        memcpy(&comp_int_val, cond.rhsValue.data, sizeof(int));
                        if(int_val == comp_int_val){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeReal:
                        float float_val;
                        memcpy(&float_val, tmp_data, sizeof(float));
                        float comp_float_val;
                        memcpy(&comp_float_val, cond.rhsValue.data, sizeof(float));
                        if(float_eq(float_val, comp_float_val)){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data, sizeof(int));
                        int comp_length;
                        memcpy(&comp_length, cond.rhsValue.data, sizeof(int));
                        if(length != comp_length) return getNextTuple(data);
                        if(memcmp((char*)tmp_data+sizeof(int), (char*)cond.rhsValue.data+sizeof(int), length) == 0){
                            free(tmp_data);
                            tmp_data = NULL;
                            return rc;
                        } else{
                            free(tmp_data);
                            tmp_data = NULL;
                            return getNextTuple(data);
                        }

                    default:{
                        std::cerr << "should never reach here" << std::endl;
                        free(tmp_data);
                        tmp_data = NULL;
                        return -11;
                    }
                }
            }

            break;

        default:
            //No_op
            return rc;
    }
    return rc;
}

void Filter::getAttributes(std::vector<Attribute> &attrs) const{
    attrs.clear();
    iter->getAttributes(attrs);
}

//Project
Project::Project(Iterator *input, const std::vector<std::string> &attrNames){
    iter = input;
    this->attrNames = attrNames;
    iter->getAttributes(attrs);
    resetIter();
}

RC Project::getNextTuple(void *data){
    RC rc = iter->getNextTuple(data);
    if(rc == QE_EOF) return rc;
    void* tmp_data = malloc(BUFFSIZE);
    int null_size = getActualByteForNullsIndicator(attrNames.size());
    memcpy(tmp_data, data, null_size);
    int offset = null_size, flag = 0, tmp_offset = null_size;
    for(int j=0; j< attrNames.size(); j++){
        offset = null_size;
        for(int i=0; i< attrs.size(); i++) {
            if (attrs[i].name == attrNames[j]) {
                flag = 1;
            }
            if(flag == 0){
                // not in attrNames, offset changed.
                switch (attrs[i].type) {
                    case TypeInt:
                        offset += sizeof(int);
                        break;
                    case TypeReal:
                        offset += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char *) data + offset, sizeof(int));
                        offset += sizeof(int) + length;
                        break;
                }
            } else {
                // in attrNames, put in tmp_data.
                switch (attrs[i].type) {
                    case TypeInt:
                        memcpy((char*)tmp_data+tmp_offset, (char *) data + offset, sizeof(int));
                        offset += sizeof(int);
                        tmp_offset += sizeof(int);
                        break;
                    case TypeReal:
                        memcpy((char*)tmp_data+tmp_offset, (char *) data + offset, sizeof(float));
                        offset += sizeof(float);
                        tmp_offset += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char *) data + offset, sizeof(int));
                        memcpy((char*)tmp_data+tmp_offset, (char *) data + offset, sizeof(int) + length);
                        offset += sizeof(int) + length;
                        tmp_offset += sizeof(int) + length;
                        break;
                }
                flag = 0;
                break;
            }
        }
    }
    memcpy(data, tmp_data, tmp_offset);
    free(tmp_data);
    tmp_data = NULL;
    return rc;
}

void Project::getAttributes(std::vector<Attribute> &attrs) const {
    attrs.clear();
    std::vector<Attribute> tmp_attrs;
    iter->getAttributes(tmp_attrs);
    for(int i=0; i<tmp_attrs.size(); i++){
        for(int j=0; j<attrNames.size(); j++){
            if(tmp_attrs[i].name == attrNames[j]){
                attrs.push_back(tmp_attrs[i]);
                break;
            }
        }
    }
}


// for BNL, malloc a buffer, load numPages * PAGESIZE data into block, loop through right, join and output.
// under some condition, i.e., cannot find match, TBD, reload next block from left then compare with right.
// do not need to load from left every time that getNextTuple is called.
// also have 1 page for right and 1 page for output.


BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
        TableScan *rightIn,           // TableScan Iterator of input S
        const Condition &condition,   // Join condition
        const unsigned numPages       // # of pages that can be loaded into memory,
        //   i.e., memory block size (decided by the optimizer)
        ){
    left_size = PAGE_SIZE*numPages;
    right_buffer = (char*)malloc(PAGE_SIZE);
    //output_buffer = (char*)malloc(PAGE_SIZE);
    left_buffer = (char*)malloc(left_size);
    left_iter = leftIn;
    right_iter = rightIn;
    cond = condition;
    left_iter->getAttributes(left_attrs);
    right_iter->getAttributes(right_attrs);
    for(int i=0; i<left_attrs.size(); i++){
        total_attrs.push_back(left_attrs[i]);
        if(left_attrs[i].name == cond.lhsAttr){
            attrType = left_attrs[i].type;
            left_attr_index = i;
        }
    }
    error = false;
    for(int i=0; i<right_attrs.size(); i++){
        total_attrs.push_back(right_attrs[i]);
        if(right_attrs[i].name == cond.rhsAttr){
            right_attr_index = i;
            if(right_attrs[i].type != attrType) {
                std::cerr << "left and right attribute not same type" << std::endl;
                error = true;
            }
        }
    }
    right_reset = false;
    left_load_next = true;
    right_load_next = true;
    left_offset = 0;
    right_offset = 0;
    cur_left_index = 0;
    cur_right_index = 0;
    left_null_size = getActualByteForNullsIndicator(left_attrs.size());
    right_null_size = getActualByteForNullsIndicator(right_attrs.size());
    total_null_size = getActualByteForNullsIndicator(total_attrs.size());
    left_done = false;
    right_done = false;
    if(DEBUG) std::cout << "attrtype: " <<  attrType << std::endl;
    if(DEBUG) std::cout << "total attr size " <<  total_attrs.size() << std::endl;
    resetIter();
}

RC BNLJoin::getNextTuple(void *data){

    //load next left block
    if(error) return QE_EOF;
    if(left_load_next){
        left_bi.clear();
        right_reset = true;
        left_load_next = false;
        left_offset = 0;
        cur_left_index = 0;
        // #TODO: here assuming single data does not exceed size of 200 as defined in testcase.
        // #TODO: add a iter->cur_index -= 1 function then can use actual size, not 200.
        char* tmp_left_data = (char*)malloc(BUFFSIZE);
        while(left_size - left_offset > BUFFSIZE){
            int data_length = left_null_size;
            BNLInfo bi;
            //memset(tmp_left_data, 0, BUFFSIZE);
            if(left_iter->getNextTuple(tmp_left_data) == QE_EOF) {
                left_done = true;
                break;
            }
            //get data length
            for(int i=0; i< left_attrs.size(); i++){
                if(i == left_attr_index) bi.attr_offset = left_offset + data_length;
                switch (left_attrs[i].type){
                    case TypeInt:
                        data_length += sizeof(int);
                        break;
                    case TypeReal:
                        data_length += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char*)tmp_left_data+data_length, sizeof(int));
                        data_length += sizeof(int) + length;
                        break;
                }
            }
            memcpy(left_buffer + left_offset, tmp_left_data, data_length);
            bi.offset = left_offset;
            bi.length = data_length;
            left_bi.push_back(bi);
            left_offset += data_length;
        }
        free(tmp_left_data);
        tmp_left_data = NULL;
    }
    // after loading next left block, reset right from the beginning
    if(right_reset){
        right_reset = false;
        right_iter->iter->si.cur_index = 0;
        right_load_next = true;
        memset(right_buffer, 0, PAGE_SIZE);
    }
    if(right_load_next){
        right_bi.clear();
        right_load_next = false;
        right_offset = 0;
        cur_right_index = 0;
        // #TODO: here assuming single data does not exceed size of 200 as defined in testcase.
        // #TODO: add a iter->cur_index -= 1 function then can use actual size, not 200.
        char* tmp_right_data = (char*)malloc(BUFFSIZE);
        while(PAGE_SIZE - right_offset > BUFFSIZE){
            int data_length = right_null_size;
            BNLInfo bi;
            //memset(tmp_right_data, 0, BUFFSIZE);
            if(right_iter->getNextTuple(tmp_right_data) == QE_EOF) {
                right_done = true;
                break;
            }
            //get data length
            for(int i=0; i< right_attrs.size(); i++){
                if(i == right_attr_index) bi.attr_offset = right_offset + data_length;
                switch (right_attrs[i].type){
                    case TypeInt:
                        data_length += sizeof(int);
                        break;
                    case TypeReal:
                        data_length += sizeof(int);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char*)tmp_right_data+data_length, sizeof(int));
                        data_length += sizeof(int) + length;
                        break;
                }
            }
            memcpy(right_buffer + right_offset, tmp_right_data, data_length);
            bi.offset = right_offset;
            bi.length = data_length;
            right_bi.push_back(bi);
            right_offset += data_length;
        }
        free(tmp_right_data);
        tmp_right_data = NULL;
    }

    //if(DEBUG) std::cout << "left bi size " << left_bi.size() << std::endl;
    //if(DEBUG) std::cout << "right bi size " <<  right_bi.size() << std::endl;

    int output_offset = 0;
    for(;cur_left_index < left_bi.size(); cur_left_index++){
        for(;cur_right_index< right_bi.size(); cur_right_index++){
            switch (cond.op){
                case EQ_OP:
                    if(!cond.bRhsIsAttr){
                        std::cerr << "has not implemented yet" << std::endl;
                    }
                    else{
                        switch (attrType){
                            case TypeInt:
                                int left_int_val;
                                memcpy(&left_int_val, left_buffer+left_bi[cur_left_index].attr_offset, sizeof(int));
                                int right_int_val;
                                memcpy(&right_int_val, right_buffer+right_bi[cur_right_index].attr_offset, sizeof(int));
                                if(DEBUG) std::cout << "int val: " <<left_int_val << " "<<right_int_val << std::endl;
                                if(left_int_val == right_int_val){

                                    unsigned char left_null, right_null, null;
                                    memset(&null, 0, total_null_size);
                                    memset(&left_null, 0, left_null_size);
                                    memset(&right_null, 0, right_null_size);
                                    memcpy(&left_null, left_buffer+left_bi[cur_left_index].offset, left_null_size);
                                    memcpy(&right_null, right_buffer+right_bi[cur_right_index].offset, right_null_size);
                                    if(total_null_size == 1){
                                        null |= left_null;
                                        null |= (right_null >> left_attrs.size());
                                    }else{
                                        std::cerr << "total attr size larger than 8, null bit not implemented" << std::endl;
                                    }
                                    memcpy((char*)data+output_offset, &null, total_null_size);
                                    output_offset += total_null_size;

                                    memcpy((char*)data+output_offset, left_buffer+left_bi[cur_left_index].offset+left_null_size,
                                           left_bi[cur_left_index].length-left_null_size);
                                    output_offset += left_bi[cur_left_index].length-left_null_size;
                                    memcpy((char*)data+output_offset, right_buffer+right_bi[cur_right_index].offset+right_null_size,
                                           right_bi[cur_right_index].length-right_null_size);
                                    cur_right_index += 1;
                                    return 0;
                                } else{
                                    break;
                                }

                            case TypeReal:
                                float left_float_val;
                                memcpy(&left_float_val, left_buffer+left_bi[cur_left_index].attr_offset, sizeof(float));
                                float right_float_val;
                                memcpy(&right_float_val, right_buffer+right_bi[cur_right_index].attr_offset, sizeof(float));
                                if(float_eq(left_float_val, right_float_val)){

                                    unsigned char left_null, right_null, null;
                                    memset(&null, 0, total_null_size);
                                    memset(&left_null, 0, left_null_size);
                                    memset(&right_null, 0, right_null_size);
                                    memcpy(&left_null, left_buffer+left_bi[cur_left_index].offset, left_null_size);
                                    memcpy(&right_null, right_buffer+right_bi[cur_right_index].offset, right_null_size);
                                    if(total_null_size == 1){
                                        null |= left_null;
                                        null |= (right_null >> left_attrs.size());
                                    }else{
                                        std::cerr << "total attr size larger than 8, null bit not implemented" << std::endl;
                                    }
                                    memcpy((char*)data+output_offset, &null, total_null_size);
                                    output_offset += total_null_size;

                                    memcpy((char*)data+output_offset, left_buffer+left_bi[cur_left_index].offset+left_null_size,
                                           left_bi[cur_left_index].length-left_null_size);
                                    output_offset += left_bi[cur_left_index].length-left_null_size;
                                    memcpy((char*)data+output_offset, right_buffer+right_bi[cur_right_index].offset+right_null_size,
                                           right_bi[cur_right_index].length-right_null_size);
                                    cur_right_index += 1;
                                    return 0;
                                } else{
                                    break;
                                }

                            case TypeVarChar:
                                float left_length;
                                memcpy(&left_length, left_buffer+left_bi[cur_left_index].attr_offset, sizeof(int));
                                float right_length;
                                memcpy(&right_length, right_buffer+right_bi[cur_right_index].attr_offset, sizeof(int));

                                if(left_length == right_length and
                                    memcmp(left_buffer+left_bi[cur_left_index].attr_offset+ sizeof(int),
                                            right_buffer+right_bi[cur_right_index].attr_offset+sizeof(int), left_length) == 0){

                                    unsigned char left_null, right_null, null;
                                    memset(&null, 0, total_null_size);
                                    memset(&left_null, 0, left_null_size);
                                    memset(&right_null, 0, right_null_size);
                                    memcpy(&left_null, left_buffer+left_bi[cur_left_index].offset, left_null_size);
                                    memcpy(&right_null, right_buffer+right_bi[cur_right_index].offset, right_null_size);
                                    if(total_null_size == 1){
                                        null |= left_null;
                                        null |= (right_null >> left_attrs.size());
                                    }else{
                                        std::cerr << "total attr size larger than 8, null bit not implemented" << std::endl;
                                    }
                                    memcpy((char*)data+output_offset, &null, total_null_size);
                                    output_offset += total_null_size;

                                    memcpy((char*)data+output_offset, left_buffer+left_bi[cur_left_index].offset+left_null_size,
                                           left_bi[cur_left_index].length-left_null_size);
                                    output_offset += left_bi[cur_left_index].length-left_null_size;
                                    memcpy((char*)data+output_offset, right_buffer+right_bi[cur_right_index].offset+right_null_size,
                                           right_bi[cur_right_index].length-right_null_size);
                                    cur_right_index += 1;
                                    return 0;
                                } else{
                                    break;
                                }

                            default:{
                                //std::cerr << "should never reach here" << std::endl;
                                std::cerr << "other ops not implemented" << std::endl;
                                return -11;
                            }
                        }
                    }

                    break;

                default:
                    break;

            }
        }
        cur_right_index = 0;
    }
    if(right_done){
        right_done = false;
        if(left_done) return QE_EOF;
        else{
            left_load_next = true;
            if(DEBUG) std::cout << "left next "<< std::endl;
            return getNextTuple(data);
        }
    }
    right_load_next = true;
    if(DEBUG) std::cout << "right next "<< std::endl;
    return getNextTuple(data);

}

void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const{
    for(int i=0; i<total_attrs.size(); i++){
        attrs.push_back(total_attrs[i]);
    }
}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
    IndexScan *rightIn,          // IndexScan Iterator of input S
    const Condition &condition){   // Join condition
    left_iter = leftIn;
    right_iter = rightIn;
    cond = condition;
    left_buffer = (char*)malloc(BUFFSIZE);
    right_buffer = (char*)malloc(BUFFSIZE);
    left_iter->getAttributes(left_attrs);
    right_iter->getAttributes(right_attrs);
    for(int i=0; i<left_attrs.size(); i++){
        total_attrs.push_back(left_attrs[i]);
        if(left_attrs[i].name == cond.lhsAttr){
            attrType = left_attrs[i].type;
            left_attr_index = i;
        }
    }
    error = false;
    for(int i=0; i<right_attrs.size(); i++) {
        total_attrs.push_back(right_attrs[i]);
        if (right_attrs[i].name == cond.rhsAttr) {
            right_attr_index = i;
            if (right_attrs[i].type != attrType) {
                std::cerr << "left and right attribute not same type" << std::endl;
                error = true;
            }
        }
    }
    left_null_size = getActualByteForNullsIndicator(left_attrs.size());
    right_null_size = getActualByteForNullsIndicator(right_attrs.size());
    total_null_size = getActualByteForNullsIndicator(total_attrs.size());
    left_size = left_null_size;
    right_size = right_null_size;
    resetIter();
}

RC INLJoin::getNextTuple(void *data){
    if(error) return QE_EOF;
    while(left_iter->getNextTuple(left_buffer)!=QE_EOF){
        int left_offset = left_null_size;
        right_iter->iter->si.cur_index = 0;
        //move left offset to left compare attr and take total size
        left_size = left_null_size;
        for (int i = 0; i < left_attrs.size(); i++) {
            if(i<left_attr_index){
                switch (left_attrs[i].type){
                    case TypeInt:
                        left_offset += sizeof(int);
                        left_size += sizeof(int);
                        break;
                    case TypeReal:
                        left_offset += sizeof(float);
                        left_size += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, left_buffer+left_offset, sizeof(int));
                        left_offset += sizeof(int)+length;
                        left_size += sizeof(int)+length;
                        break;
                }
            }
            else{
                switch (left_attrs[i].type){
                    case TypeInt:
                        left_size += sizeof(int);
                        break;
                    case TypeReal:
                        left_size += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, left_buffer+left_size, sizeof(int));
                        left_size += sizeof(int)+length;
                        break;
                }
            }
        }
        while(right_iter->getNextTuple(right_buffer)!=QE_EOF){
            if(DEBUG) std::cout << "right loop: " << right_iter->iter->si.cur_index <<std::endl;
            int right_offset = right_null_size;
            right_size = right_null_size;
            int output_offset = 0;
            //move right offset to right compare attr
            for (int i = 0; i < right_attrs.size(); i++) {
                if(i<right_attr_index){
                    switch (right_attrs[i].type){
                        case TypeInt:
                            right_offset += sizeof(int);
                            right_size += sizeof(int);
                            break;
                        case TypeReal:
                            right_offset += sizeof(float);
                            right_size += sizeof(float);
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy(&length, right_buffer+right_offset, sizeof(int));
                            right_offset += sizeof(int)+length;
                            right_size += sizeof(int)+length;
                            break;
                    }
                }
                else{
                    switch (right_attrs[i].type){
                        case TypeInt:
                            right_size += sizeof(int);
                            break;
                        case TypeReal:
                            right_size += sizeof(float);
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy(&length, right_buffer+right_size, sizeof(int));
                            right_size += sizeof(int)+length;
                            break;
                    }
                }
            }
            switch (cond.op){
                case EQ_OP:
                    if(!cond.bRhsIsAttr){
                        std::cerr << "has not implemented yet" << std::endl;
                    }
                    else {
                        switch (attrType) {
                            case TypeInt:
                                int left_int_val, right_int_val;
                                memcpy(&left_int_val, left_buffer+left_offset, sizeof(int));
                                memcpy(&right_int_val, right_buffer+right_offset, sizeof(int));
                                if(left_int_val == right_int_val){
                                    if(DEBUG) std::cout << "eq int found" << left_int_val << " " << right_int_val  << std::endl;

                                    unsigned char left_null, right_null, null;
                                    memset(&null, 0, total_null_size);
                                    memset(&left_null, 0, left_null_size);
                                    memset(&right_null, 0, right_null_size);
                                    memcpy(&left_null, left_buffer, left_null_size);
                                    memcpy(&right_null, right_buffer, right_null_size);
                                    if(total_null_size == 1){
                                        null |= left_null;
                                        null |= (right_null >> left_attrs.size());
                                    }else{
                                        std::cerr << "total attr size larger than 8, null bit not implemented" << std::endl;
                                    }
                                    memcpy((char*)data+output_offset, &null, total_null_size);
                                    output_offset += total_null_size;

                                    memcpy((char*)data+output_offset, left_buffer+left_null_size,
                                           left_size-left_null_size);
                                    output_offset += left_size-left_null_size;
                                    memcpy((char*)data+output_offset, right_buffer+right_null_size,
                                           right_size-right_null_size);
                                    return 0;
                                } else{
                                    break;
                                }

                            case TypeReal:
                                float left_float_val, right_float_val;
                                memcpy(&left_float_val, left_buffer+left_offset, sizeof(float));
                                memcpy(&right_float_val, right_buffer+right_offset, sizeof(float));
                                if(float_eq(left_float_val, right_float_val)){
                                    if(DEBUG) std::cout << "eq float found: " <<left_float_val << " " << right_float_val  << std::endl;

                                    unsigned char left_null, right_null, null;
                                    memset(&null, 0, total_null_size);
                                    memset(&left_null, 0, left_null_size);
                                    memset(&right_null, 0, right_null_size);
                                    memcpy(&left_null, left_buffer, left_null_size);
                                    memcpy(&right_null, right_buffer, right_null_size);
                                    if(DEBUG) std::cout << "eq float found bp1" << std::endl;
                                    if(total_null_size == 1){
                                        null |= left_null;
                                        null |= (right_null >> left_attrs.size());
                                    }else{
                                        std::cerr << "total attr size larger than 8, null bit not implemented" << std::endl;
                                    }
                                    memcpy((char*)data+output_offset, &null, total_null_size);
                                    output_offset += total_null_size;
                                    if(DEBUG) std::cout << "eq float found bp2" << std::endl;
                                    if(DEBUG) std::cout << "left size: " << left_size << std::endl;
                                    memcpy((char*)data+output_offset, left_buffer+left_null_size,
                                           left_size-left_null_size);
                                    output_offset += left_size-left_null_size;
                                    if(DEBUG) std::cout << "eq float found bp3" << std::endl;
                                    if(DEBUG) std::cout << "right size: " << right_size << std::endl;
                                    memcpy((char*)data+output_offset, right_buffer+right_null_size,
                                           right_size-right_null_size);
                                    if(DEBUG) std::cout << "eq float found bp4" << std::endl;
                                    return 0;
                                } else{
                                    break;
                                }

                            case TypeVarChar:
                                int left_length, right_length;
                                memcpy(&left_length, left_buffer+left_offset, sizeof(int));
                                memcpy(&right_length, right_buffer+right_offset, sizeof(int));
                                if(left_length != right_length) break;
                                if(memcmp(left_buffer+left_offset+sizeof(int), right_buffer+right_offset+sizeof(int), left_length)==0){
                                    if(DEBUG) std::cout << "eq varchar found" << std::endl;

                                    unsigned char left_null, right_null, null;
                                    memset(&null, 0, total_null_size);
                                    memset(&left_null, 0, left_null_size);
                                    memset(&right_null, 0, right_null_size);
                                    memcpy(&left_null, left_buffer, left_null_size);
                                    memcpy(&right_null, right_buffer, right_null_size);
                                    if(total_null_size == 1){
                                        null |= left_null;
                                        null |= (right_null >> left_attrs.size());
                                    }else{
                                        std::cerr << "total attr size larger than 8, null bit not implemented" << std::endl;
                                    }
                                    memcpy((char*)data+output_offset, &null, total_null_size);
                                    output_offset += total_null_size;

                                    memcpy((char*)data+output_offset, left_buffer+left_null_size,
                                           left_size-left_null_size);
                                    output_offset += left_size-left_null_size;
                                    memcpy((char*)data+output_offset, right_buffer+right_null_size,
                                           right_size-right_null_size);
                                    return 0;
                                } else{
                                    break;
                                }

                        }
                    }
                    break;

                default:
                    std::cerr << "other op not implemented" << std::endl;
                    return QE_EOF;
            }
        }
    }
    return QE_EOF;
}

// For attribute in std::vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(std::vector<Attribute> &attrs) const{

}

//leave GHJoin for last
GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
        Iterator *rightIn,               // Iterator of input S
        const Condition &condition,      // Join condition (CompOp is always EQ)
        const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        ){

}

RC GHJoin::getNextTuple(void *data){

}

// For attribute in std::vector<Attribute>, name it as rel.attr
void GHJoin::getAttributes(std::vector<Attribute> &attrs) const{

}

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
        const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
        AggregateOp op            // Aggregate operation
        ){
    iter = input;
    this->op = op;
    this->aggAttr = aggAttr;
    iter->getAttributes(attrs);
    null_size = getActualByteForNullsIndicator(attrs.size());
    null_val = (unsigned char*)malloc(null_size);
    memset(null_val, 0, null_size);
    iter->resetIter();
    int_max = 0;
    float_max = 0.0;
    max_init = false;
    int_min = 0;
    float_min = 0.0;
    min_init = false;
    done = false;
    avg = 0.0;
    avg_num = 0;
    sum = 0;
    count = 0;
    group = false;
}

// Optional for everyone: 5 extra-credit points
// Group-based hash aggregation
Aggregate::Aggregate(Iterator *input,             // Iterator of input R
        const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
        const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
        AggregateOp op              // Aggregate operation
        ){
    iter = input;
    this->op = op;
    this->aggAttr = aggAttr;
    this->gAttr = groupAttr;
    iter->getAttributes(attrs);
    null_size = getActualByteForNullsIndicator(attrs.size());
    null_val = (unsigned char*)malloc(null_size);
    memset(null_val, 0, null_size);
    iter->resetIter();
    done = false;
    group = true;
    hash_init = false;
}

RC Aggregate::getNextTuple(void *data){
    if(done){
        done = false;
        return QE_EOF;
    }
    char* tmp_data = (char*)malloc(BUFFSIZE);
    int tmp_length;
    int tmp_index;
    if(!group) {
        while (true) {
            RC rc = iter->getNextTuple(tmp_data);

            tmp_length = null_size;
            tmp_index = null_size;
            for (int i = 0; i < attrs.size(); i++) {
                if (attrs[i].name == aggAttr.name) {
                    tmp_index = tmp_length;
                }
                switch (attrs[i].type) {
                    case TypeInt:
                        tmp_length += sizeof(int);
                        break;
                    case TypeReal:
                        tmp_length += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data + tmp_length, sizeof(int));
                        tmp_length += length + sizeof(int);
                        break;
                }
            }

            switch (op) {
                case MAX:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                float int2float = float(int_max);
                                memcpy((char *) data + null_size, &int2float, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));
                            if (!max_init) {
                                int_max = tmp_int;
                                max_init = true;
                            } else {
                                int_max = int_max > tmp_int ? int_max : tmp_int;
                            }
                            //std::cout<<"int_max and tmp_int: "<<int_max<<" "<<tmp_int<<std::endl;
                            //std::cout<<"new max: "<<int_max<<std::endl;

                            break;
                        case TypeReal:
                            float tmp_float;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &float_max, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));
                            if (!max_init) {
                                float_max = tmp_float;
                                max_init = true;
                            } else {
                                float_max = float_max > tmp_float ? float_max : tmp_float;
                            }
                            std::cout << "new max: " << float_max << std::endl;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }

                    break;
                case MIN:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                float int2float = float(int_min);
                                memcpy((char *) data + null_size, &int2float, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));
                            if (!min_init) {
                                int_min = tmp_int;
                                min_init = true;
                            } else {
                                int_min = int_min < tmp_int ? int_min : tmp_int;
                            }

                            break;
                        case TypeReal:
                            float tmp_float;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &float_min, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));
                            if (!min_init) {
                                float_min = tmp_float;
                                min_init = true;
                            } else {
                                float_min = float_min < tmp_float ? float_min : tmp_float;
                            }

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }

                    break;
                case SUM:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &sum, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));

                            sum += float(tmp_int);

                            break;
                        case TypeReal:
                            float tmp_float;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &sum, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));

                            sum += tmp_float;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }

                    break;
                case AVG:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &avg, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));

                            avg = (avg * (float) avg_num + float(tmp_int)) / (float) (avg_num + 1);
                            avg_num += 1;

                            break;
                        case TypeReal:
                            float tmp_float;
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &avg, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));

                            avg = (avg * (float) avg_num + tmp_float) / (float) (avg_num + 1);
                            avg_num += 1;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }

                    break;
                case COUNT:
                    switch (aggAttr.type) {
                        case TypeInt:
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &count, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }
                            count += 1;

                            break;
                        case TypeReal:
                            if (rc == QE_EOF) {
                                memcpy(data, null_val, null_size);
                                memcpy((char *) data + null_size, &count, sizeof(float));
                                free(tmp_data);
                                tmp_data = NULL;
                                done = true;
                                return 0;
                            }
                            count += 1;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }
                    break;
                default:
                    std::cerr << "should never reach here. aggregate::getNextTuple" << std::endl;
                    break;
            }
            if (rc == QE_EOF) {
                free(tmp_data);
                tmp_data = NULL;
                return QE_EOF;
            }
        }
    } else{

        //#TODO: add group by. add to hash with key. grp_offset it already located
        while (true) {
            RC rc = iter->getNextTuple(tmp_data);

            if (rc == QE_EOF) {
                memcpy(data, null_val, null_size);
                int data_offset= null_size;

                if(!hash_init){
                    switch (gAttr.type){
                        case TypeInt:
                            int_hash_iter = int_hash.begin();
                            hash_init = true;
                            break;
                        case TypeReal:
                            real_hash_iter = real_hash.begin();
                            hash_init = true;
                            break;
                        case TypeVarChar:
                            varchar_hash_iter = varchar_hash.begin();
                            hash_init = true;
                            break;
                    }
                }
                float output_val;
                switch (gAttr.type) {
                    case TypeInt: {
                        int output_int = int_hash_iter->first;
                        memcpy((char *) data + data_offset, &output_int, sizeof(int));
                        data_offset += sizeof(int);
                        output_val = int_hash_iter->second;
                        break;
                    }
                    case TypeReal: {
                        float output_float = real_hash_iter->first;
                        memcpy((char *) data + data_offset, &output_float, sizeof(float));
                        data_offset += sizeof(float);
                        output_val = int_hash_iter->second;
                        break;
                    }
                    case TypeVarChar: {
                        int length = varchar_hash_iter->first.size();
                        memcpy((char *) data + data_offset, &length, sizeof(int));
                        data_offset += sizeof(int);
                        memcpy((char *) data + data_offset, varchar_hash_iter->first.c_str(), length );
                        data_offset += length;
                        output_val = int_hash_iter->second;
                        break;
                    }
                }
                memcpy((char *) data + data_offset, &output_val, sizeof(float));

                switch (gAttr.type){
                    case TypeInt:
                        int_hash_iter++;
                        if(int_hash_iter == int_hash.end()){
                            free(tmp_data);
                            tmp_data = NULL;
                            done = true;
                            return 0;
                        }
                        break;
                    case TypeReal:
                        real_hash_iter++;
                        if(real_hash_iter == real_hash.end()){
                            free(tmp_data);
                            tmp_data = NULL;
                            done = true;
                            return 0;
                        }
                        break;
                    case TypeVarChar:
                        varchar_hash_iter++;
                        if(varchar_hash_iter == varchar_hash.end()){
                            free(tmp_data);
                            tmp_data = NULL;
                            done = true;
                            return 0;
                        }
                        break;
                }
                return 0;
            }


            int hash_offset = null_size;
            for (int i = 0; i < attrs.size(); i++) {
                if (attrs[i].name == gAttr.name) {
                    break;
                }
                switch (attrs[i].type) {
                    case TypeInt:
                        hash_offset += sizeof(int);
                        break;
                    case TypeReal:
                        hash_offset += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data + hash_offset, sizeof(int));
                        hash_offset += length + sizeof(int);
                        break;
                }
            }

            tmp_length = null_size;
            tmp_index = null_size;
            for (int i = 0; i < attrs.size(); i++) {
                if (attrs[i].name == aggAttr.name) {
                    tmp_index = tmp_length;
                }
                switch (attrs[i].type) {
                    case TypeInt:
                        tmp_length += sizeof(int);
                        break;
                    case TypeReal:
                        tmp_length += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, tmp_data + tmp_length, sizeof(int));
                        tmp_length += length + sizeof(int);
                        break;
                }
            }
            float val = 0.0;
            switch (op) {
                case MAX:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));
                            val = float(tmp_int);

                            break;
                        case TypeReal:
                            float tmp_float;

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));
                            val = tmp_float;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }
                    switch (gAttr.type) {
                        case TypeInt:
                            int hash_int;
                            memcpy(&hash_int, tmp_data + hash_offset, sizeof(int));
                            if (int_hash.find(hash_int) == int_hash.end()) {
                                int_hash[hash_int] = val;
                            } else {
                                int_hash[hash_int] = int_hash[hash_int] > val ? int_hash[hash_int] : val;
                            }
                            break;
                        case TypeReal:
                            int hash_real;
                            memcpy(&hash_real, tmp_data + hash_offset, sizeof(float));
                            if (real_hash.find(hash_real) == real_hash.end()) {
                                real_hash[hash_real] = val;
                            } else {
                                real_hash[hash_real] = real_hash[hash_real] > val ? real_hash[hash_real] : val;
                            }
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy(&length, tmp_data + hash_offset, sizeof(int));
                            char* tmp_string_ptr = (char*)malloc(sizeof(length));
                            memcpy(tmp_string_ptr, tmp_data+hash_offset+ sizeof(int), length);
                            std::string hash_string = std::string(tmp_string_ptr, tmp_string_ptr+length);
                            if (varchar_hash.find(hash_string) == varchar_hash.end()) {
                                varchar_hash[hash_string] = val;
                            } else {
                                varchar_hash[hash_string] = varchar_hash[hash_string] > val ? varchar_hash[hash_string] : val;
                            }
                            free(tmp_string_ptr);
                            tmp_string_ptr = NULL;
                            break;
                    }

                    break;
                case MIN:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));
                            val = float(tmp_int);

                            break;
                        case TypeReal:
                            float tmp_float;

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));
                            val = tmp_float;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }
                    switch (gAttr.type) {
                        case TypeInt:
                            int hash_int;
                            memcpy(&hash_int, tmp_data + hash_offset, sizeof(int));
                            if (int_hash.find(hash_int) == int_hash.end()) {
                                int_hash[hash_int] = val;
                            } else {
                                int_hash[hash_int] = int_hash[hash_int] < val ? int_hash[hash_int] : val;
                            }
                            break;
                        case TypeReal:
                            int hash_real;
                            memcpy(&hash_real, tmp_data + hash_offset, sizeof(float));
                            if (real_hash.find(hash_real) == real_hash.end()) {
                                real_hash[hash_real] = val;
                            } else {
                                real_hash[hash_real] = real_hash[hash_real] < val ? real_hash[hash_real] : val;
                            }
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy(&length, tmp_data + hash_offset, sizeof(int));
                            char* tmp_string_ptr = (char*)malloc(sizeof(length));
                            memcpy(tmp_string_ptr, tmp_data+hash_offset+ sizeof(int), length);
                            std::string hash_string = std::string(tmp_string_ptr, tmp_string_ptr+length);
                            if (varchar_hash.find(hash_string) == varchar_hash.end()) {
                                varchar_hash[hash_string] = val;
                            } else {
                                varchar_hash[hash_string] = varchar_hash[hash_string] < val ? varchar_hash[hash_string] : val;
                            }
                            free(tmp_string_ptr);
                            tmp_string_ptr = NULL;
                            break;
                    }

                    break;
                case SUM:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));
                            val = float(tmp_int);

                            break;
                        case TypeReal:
                            float tmp_float;

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));
                            val = tmp_float;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }
                    switch (gAttr.type) {
                        case TypeInt:
                            int hash_int;
                            memcpy(&hash_int, tmp_data + hash_offset, sizeof(int));
                            if (int_hash.find(hash_int) == int_hash.end()) {
                                int_hash[hash_int] = val;
                            } else {
                                int_hash[hash_int] += val;
                            }
                            break;
                        case TypeReal:
                            int hash_real;
                            memcpy(&hash_real, tmp_data + hash_offset, sizeof(float));
                            if (real_hash.find(hash_real) == real_hash.end()) {
                                real_hash[hash_real] = val;
                            } else {
                                real_hash[hash_real] += val;
                            }
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy(&length, tmp_data + hash_offset, sizeof(int));
                            char* tmp_string_ptr = (char*)malloc(sizeof(length));
                            memcpy(tmp_string_ptr, tmp_data+hash_offset+ sizeof(int), length);
                            std::string hash_string = std::string(tmp_string_ptr, tmp_string_ptr+length);
                            if (varchar_hash.find(hash_string) == varchar_hash.end()) {
                                varchar_hash[hash_string] = val;
                            } else {
                                varchar_hash[hash_string] += val;
                            }
                            free(tmp_string_ptr);
                            tmp_string_ptr = NULL;
                            break;
                    }
                    break;
                case AVG:
                    switch (aggAttr.type) {
                        case TypeInt:
                            int tmp_int;

                            memcpy(&tmp_int, tmp_data + tmp_index, sizeof(int));
                            val = float(tmp_int);

                            break;
                        case TypeReal:
                            float tmp_float;

                            memcpy(&tmp_float, tmp_data + tmp_index, sizeof(float));
                            val = tmp_float;

                            break;
                        case TypeVarChar:
                            std::cerr << "aggregate should not operate on varchar" << std::endl;
                            break;
                    }
                    switch (gAttr.type) {
                        case TypeInt:
                            int hash_int;
                            memcpy(&hash_int, tmp_data + hash_offset, sizeof(int));
                            if (int_hash.find(hash_int) == int_hash.end()) {
                                int_hash[hash_int] = val;
                                int_avg_num[hash_int] = 1;
                            } else {
                                int_avg_num[hash_int] += 1;
                                int_hash[hash_int] += (val-int_hash[hash_int]) / int_avg_num[hash_int];
                            }
                            break;
                        case TypeReal:
                            int hash_real;
                            memcpy(&hash_real, tmp_data + hash_offset, sizeof(float));
                            if (real_hash.find(hash_real) == real_hash.end()) {
                                real_hash[hash_real] = val;
                                real_avg_num[hash_real] = 1;
                            } else {
                                real_avg_num[hash_real] += 1;
                                real_hash[hash_real] = (val-real_hash[hash_real]) / real_avg_num[hash_real];
                            }
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy(&length, tmp_data + hash_offset, sizeof(int));
                            char* tmp_string_ptr = (char*)malloc(sizeof(length));
                            memcpy(tmp_string_ptr, tmp_data+hash_offset+ sizeof(int), length);
                            std::string hash_string = std::string(tmp_string_ptr, tmp_string_ptr+length);
                            if (varchar_hash.find(hash_string) == varchar_hash.end()) {
                                varchar_hash[hash_string] = val;
                                varchar_avg_num[hash_string] = 1;
                            } else {
                                varchar_avg_num[hash_string] += 1;
                                varchar_hash[hash_string] = (val-varchar_hash[hash_string]) / varchar_avg_num[hash_string];
                            }
                            free(tmp_string_ptr);
                            tmp_string_ptr = NULL;
                            break;
                    }
                    break;
                case COUNT:
                    switch (gAttr.type) {
                        case TypeInt:
                            int hash_int;
                            memcpy(&hash_int, tmp_data + hash_offset, sizeof(int));
                            if (int_hash.find(hash_int) == int_hash.end()) {
                                int_hash[hash_int] = 1;
                            } else {
                                int_hash[hash_int] += 1;
                            }
                            break;
                        case TypeReal:
                            int hash_real;
                            memcpy(&hash_real, tmp_data + hash_offset, sizeof(float));
                            if (real_hash.find(hash_real) == real_hash.end()) {
                                real_hash[hash_real] = 1;
                            } else {
                                real_hash[hash_real] += 1;
                            }
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy(&length, tmp_data + hash_offset, sizeof(int));
                            char* tmp_string_ptr = (char*)malloc(sizeof(length));
                            memcpy(tmp_string_ptr, tmp_data+hash_offset+ sizeof(int), length);
                            std::string hash_string = std::string(tmp_string_ptr, tmp_string_ptr+length);
                            if (varchar_hash.find(hash_string) == varchar_hash.end()) {
                                varchar_hash[hash_string] = 1;
                            } else {
                                varchar_hash[hash_string] += 1;
                            }
                            free(tmp_string_ptr);
                            tmp_string_ptr = NULL;
                            break;
                    }
                    break;
                default:
                    std::cerr << "should never reach here. aggregate::getNextTuple" << std::endl;
                    break;
            }
            if (rc == QE_EOF) {
                free(tmp_data);
                tmp_data = NULL;
                return QE_EOF;
            }
        }
    }
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(std::vector<Attribute> &attrs) const{
    Attribute tmp_attr;
    switch (op){
        case MAX:
            tmp_attr.name = "MAX(" + aggAttr.name + ")";
            break;
        case MIN:
            tmp_attr.name = "MIN(" + aggAttr.name + ")";
            break;
        case SUM:
            tmp_attr.name = "SUM(" + aggAttr.name + ")";
            break;
        case AVG:
            tmp_attr.name = "AVG(" + aggAttr.name + ")";
            break;
        case COUNT:
            tmp_attr.name = "COUNT(" + aggAttr.name + ")";
            break;
        default:
            std::cerr << "should never reach here. aggregate::getNextTuple" << std::endl;
            break;
    }
    tmp_attr.type = TypeReal;
    tmp_attr.length = sizeof(float);
    attrs.push_back(tmp_attr);
}